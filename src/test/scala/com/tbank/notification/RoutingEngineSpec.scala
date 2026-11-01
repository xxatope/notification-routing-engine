package com.tbank.notification

import cats.effect.IO
import cats.syntax.all.*
import com.tbank.notification.domain.*
import com.tbank.notification.engine.{InMemoryFrequencyCapper, RoutingEngine}
import com.tbank.notification.provider.{MockPushProvider, MockSmsProvider}
import com.tbank.notification.repository.InMemoryNotificationRepository
import munit.CatsEffectSuite
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import scala.concurrent.duration.*

class RoutingEngineSpec extends CatsEffectSuite:

  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  private def buildEngine(
      pushProvider: com.tbank.notification.provider.PushProvider[IO] = MockPushProvider.make[IO](simulatedDelay = 10.millis),
      smsPrimary: com.tbank.notification.provider.SmsProvider[IO] = MockSmsProvider.make[IO]("SMS-Primary", simulatedDelay = 10.millis),
      smsSecondary: com.tbank.notification.provider.SmsProvider[IO] = MockSmsProvider.make[IO]("SMS-Backup", simulatedDelay = 10.millis),
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

  test("Atomic Idempotency: concurrent duplicate requests result in exactly one execution") {
    for
      engine <- buildEngine()
      req = NotificationRequest(
        idempotencyKey = "concurrent-key-777",
        userId = "user-2",
        priority = Priority.Critical,
        content = "Parallel test",
        phone = Some("+79998887766"),
        deviceToken = None
      )
      // Запускаем 5 конкурентных запросов с одним ключом одновременно
      results <- (1 to 5).toList.parTraverse(_ => engine.submitNotification(req))
    yield {
      val successful = results.collect { case Right(record) => record }
      assertEquals(successful.size, 5)
      // Все ответы должны иметь одинаковый UUID (одна и та же запись)
      val uniqueIds = successful.map(_.id).toSet
      assertEquals(uniqueIds.size, 1, "Expected identical record ID across all concurrent attempts")
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
