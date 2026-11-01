package com.tbank.notification

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.tbank.notification.domain.*
import com.tbank.notification.engine.{InMemoryFrequencyCapper, RoutingEngine}
import com.tbank.notification.provider.{MockPushProvider, MockSmsProvider, PushProvider, SmsProvider}
import com.tbank.notification.repository.InMemoryNotificationRepository
import munit.CatsEffectSuite
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import scala.concurrent.duration.*

class RoutingEngineSpec extends CatsEffectSuite:

  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  private def buildEngine(
      pushProvider: PushProvider[IO] = MockPushProvider.make[IO](simulatedDelay = 10.millis),
      smsPrimary: SmsProvider[IO] = MockSmsProvider.make[IO]("SMS-Primary", simulatedDelay = 10.millis),
      smsSecondary: SmsProvider[IO] = MockSmsProvider.make[IO]("SMS-Backup", simulatedDelay = 10.millis),
      maxMarketing: Int = 2
  ): IO[RoutingEngine[IO]] =
    for
      repo   <- InMemoryNotificationRepository.make[IO]
      capper <- InMemoryFrequencyCapper.make[IO](InMemoryFrequencyCapper.Config(maxMarketingPerWindow = maxMarketing))
    yield RoutingEngine.make[IO](
      pushProvider = pushProvider,
      smsPrimary = smsPrimary,
      smsSecondary = smsSecondary,
      frequencyCapper = capper,
      repository = repo,
      config = RoutingEngine.Config(pushTimeout = 1.second, smsTimeout = 1.second)
    )

  test("Deliver via Push when deviceToken is valid") {
    for
      engine <- buildEngine()
      req = NotificationRequest(
        idempotencyKey = "req-1",
        userId = "user-1",
        priority = Priority.Critical,
        content = "Your login code: 4921",
        phone = Some("+79991234567"),
        deviceToken = Some("valid-device-token-abc")
      )
      res <- engine.submitNotification(req)
    yield {
      assert(res.isRight)
      res.toOption.get.status match
        case DeliveryStatus.Delivered(channel, provider, _) =>
          assertEquals(channel, Channel.Push)
          assertEquals(provider, "APNS-FCM-Gateway")
        case other =>
          fail(s"Expected Delivered via Push, but got: $other")
    }
  }

  test("Failover to Primary SMS when Push fails") {
    for
      engine <- buildEngine()
      req = NotificationRequest(
        idempotencyKey = "req-2",
        userId = "user-1",
        priority = Priority.Critical,
        content = "Your transaction code: 7788",
        phone = Some("+79991234567"),
        deviceToken = Some("fail-transient-push-token")
      )
      res <- engine.submitNotification(req)
    yield {
      assert(res.isRight)
      res.toOption.get.status match
        case DeliveryStatus.Delivered(channel, provider, _) =>
          assertEquals(channel, Channel.Sms)
          assertEquals(provider, "SMS-Primary")
        case other =>
          fail(s"Expected Delivered via Primary SMS, but got: $other")
    }
  }

  test("Failover to Secondary SMS when both Push and Primary SMS fail") {
    val failingPrimarySms = MockSmsProvider.make[IO]("SMS-Primary", simulatedDelay = 10.millis, alwaysFail = true)
    for
      engine <- buildEngine(smsPrimary = failingPrimarySms)
      req = NotificationRequest(
        idempotencyKey = "req-3",
        userId = "user-1",
        priority = Priority.Critical,
        content = "Security alert",
        phone = Some("+79991234567"),
        deviceToken = Some("fail-transient-push-token")
      )
      res <- engine.submitNotification(req)
    yield {
      assert(res.isRight)
      res.toOption.get.status match
        case DeliveryStatus.Delivered(channel, provider, _) =>
          assertEquals(channel, Channel.Sms)
          assertEquals(provider, "SMS-Backup")
        case other =>
          fail(s"Expected Delivered via SMS-Backup, but got: $other")
    }
  }

  test("Stop cascade when Primary SMS fails with PermanentFailure") {
    val permanentFailSms = new SmsProvider[IO]:
      def name: String = "Permanent-Fail-SMS"
      def sendSms(phone: String, content: String): IO[SendResult] =
        IO.pure(SendResult.PermanentFailure("Unallocated number"))

    for
      engine <- buildEngine(smsPrimary = permanentFailSms)
      req = NotificationRequest(
        idempotencyKey = "req-permanent",
        userId = "user-perm",
        priority = Priority.Critical,
        content = "Alert",
        phone = Some("+000123456"),
        deviceToken = None
      )
      res <- engine.submitNotification(req)
    yield {
      assert(res.isRight)
      res.toOption.get.status match
        case DeliveryStatus.Failed(reason) =>
          assert(reason.contains("SMS permanently failed"))
        case other =>
          fail(s"Expected DeliveryStatus.Failed, got $other")
    }
  }

  test("Validation: reject empty content and missing destinations") {
    for
      engine <- buildEngine()
      emptyReq = NotificationRequest(
        idempotencyKey = "val-1",
        userId = "user-val",
        priority = Priority.Critical,
        content = "",
        phone = Some("+79991234567")
      )
      noDestReq = NotificationRequest(
        idempotencyKey = "val-2",
        userId = "user-val",
        priority = Priority.Critical,
        content = "Hello",
        phone = None,
        deviceToken = None
      )
      res1 <- engine.submitNotification(emptyReq)
      res2 <- engine.submitNotification(noDestReq)
    yield {
      assert(res1.isLeft)
      assert(res2.isLeft)
    }
  }

  test("Atomic Idempotency: concurrent requests trigger provider send exactly ONCE") {
    for
      callCounter <- Ref.of[IO, Int](0)
      countingPushProvider = new PushProvider[IO]:
        def sendPush(token: String, content: String): IO[SendResult] =
          callCounter.update(_ + 1) *> IO.sleep(20.millis) *> IO.pure(SendResult.Success("Counted-Push"))

      engine <- buildEngine(pushProvider = countingPushProvider)
      req = NotificationRequest(
        idempotencyKey = "concurrent-key-exact-one",
        userId = "user-parallel",
        priority = Priority.Critical,
        content = "Single delivery test",
        phone = None,
        deviceToken = Some("token-parallel")
      )
      // 5 одновременных запросов
      results <- (1 to 5).toList.parTraverse(_ => engine.submitNotification(req))
      totalCalls <- callCounter.get
    yield {
      assertEquals(totalCalls, 1, "Push provider must be invoked EXACTLY once despite concurrent requests")
      val successful = results.collect { case Right(record) => record }
      assertEquals(successful.size, 5)
      val uniqueIds = successful.map(_.id).toSet
      assertEquals(uniqueIds.size, 1)
    }
  }

  test("Sequential duplicate request returns existing completed record") {
    for
      engine <- buildEngine()
      req = NotificationRequest(
        idempotencyKey = "seq-idempotency-key",
        userId = "user-seq",
        priority = Priority.Critical,
        content = "Sequential test",
        phone = Some("+79991112233"),
        deviceToken = Some("valid-token")
      )
      first  <- engine.submitNotification(req)
      second <- engine.submitNotification(req)
    yield {
      assert(first.isRight)
      assert(second.isRight)
      assertEquals(first.toOption.get.id, second.toOption.get.id)
      assertEquals(first.toOption.get.status, second.toOption.get.status)
    }
  }

  test("Marketing notification rejected when Frequency Cap exceeded") {
    for
      engine <- buildEngine(maxMarketing = 1)
      req1 = NotificationRequest(
        idempotencyKey = "promo-1",
        userId = "user-marketing-test",
        priority = Priority.Marketing,
        content = "Promo 1",
        phone = Some("+79991112233")
      )
      req2 = NotificationRequest(
        idempotencyKey = "promo-2",
        userId = "user-marketing-test",
        priority = Priority.Marketing,
        content = "Promo 2",
        phone = Some("+79991112233")
      )
      res1 <- engine.submitNotification(req1)
      res2 <- engine.submitNotification(req2)
    yield {
      assert(res1.isRight, "First promo must succeed")
      assert(res2.isLeft, "Second promo must be rejected due to limit")
      res2.left.toOption.get match
        case AppError.RateLimitExceeded(user, limit) =>
          assertEquals(user, "user-marketing-test")
          assertEquals(limit, 1)
        case other => fail(s"Expected RateLimitExceeded, but got: $other")
    }
  }
