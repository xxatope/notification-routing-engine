package com.tbank.notification

import cats.effect.IO
import com.tbank.notification.domain.Priority
import com.tbank.notification.engine.InMemoryFrequencyCapper
import munit.CatsEffectSuite
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import scala.concurrent.duration.*

class FrequencyCapperSpec extends CatsEffectSuite:

  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  test("Critical priority notifications must never be blocked by frequency capping") {
    for
      capper  <- InMemoryFrequencyCapper.make[IO](InMemoryFrequencyCapper.Config(maxMarketingPerWindow = 2))
      results <- (1 to 10).toList.traverse(_ => capper.isAllowed("user-123", Priority.Critical))
    yield assertEquals(results.forall(_ == true), true)
  }

  test("Marketing notifications must be blocked once window limit is reached") {
    for
      capper <- InMemoryFrequencyCapper.make[IO](InMemoryFrequencyCapper.Config(maxMarketingPerWindow = 2, windowDuration = 1.hour))
      first  <- capper.isAllowed("user-456", Priority.Marketing)
      second <- capper.isAllowed("user-456", Priority.Marketing)
      third  <- capper.isAllowed("user-456", Priority.Marketing)
    yield {
      assertEquals(first, true, "First marketing message should be allowed")
      assertEquals(second, true, "Second marketing message should be allowed")
      assertEquals(third, false, "Third marketing message must be blocked by rate limit")
    }
  }

  test("Marketing limits are isolated between different users") {
    for
      capper <- InMemoryFrequencyCapper.make[IO](InMemoryFrequencyCapper.Config(maxMarketingPerWindow = 1))
      user1Attempt1 <- capper.isAllowed("user-A", Priority.Marketing)
      user1Attempt2 <- capper.isAllowed("user-A", Priority.Marketing)
      user2Attempt1 <- capper.isAllowed("user-B", Priority.Marketing)
    yield {
      assertEquals(user1Attempt1, true)
      assertEquals(user1Attempt2, false)
      assertEquals(user2Attempt1, true, "User B should not be affected by User A's limit")
    }
  }
