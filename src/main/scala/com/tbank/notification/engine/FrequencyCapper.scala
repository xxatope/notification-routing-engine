package com.tbank.notification.engine

import cats.effect.{Async, Ref}
import cats.syntax.all.*
import com.tbank.notification.domain.Priority
import org.typelevel.log4cats.LoggerFactory
import java.time.Instant
import scala.concurrent.duration.*

trait FrequencyCapper[F[_]]:
  def isAllowed(userId: String, priority: Priority): F[Boolean]

object InMemoryFrequencyCapper:
  final case class Config(
      maxMarketingPerWindow: Int = 2,
      windowDuration: FiniteDuration = 24.hours
  )

  def make[F[_]: Async: LoggerFactory](config: Config = Config()): F[FrequencyCapper[F]] =
    for
      logger <- LoggerFactory[F].create
      state  <- Ref.of[F, Map[String, Vector[Instant]]](Map.empty)
    yield new FrequencyCapper[F]:
      override def isAllowed(userId: String, priority: Priority): F[Boolean] =
        priority match
          case Priority.Critical =>
            Async[F].pure(true)

          case Priority.Marketing =>
            for
              now <- Async[F].realTimeInstant
              cutoff = now.minusMillis(config.windowDuration.toMillis)
              allowed <- state.modify { map =>
                val userTimestamps = map.getOrElse(userId, Vector.empty).filter(_.isAfter(cutoff))
                if userTimestamps.size >= config.maxMarketingPerWindow then
                  (map.updated(userId, userTimestamps), false)
                else
                  (map.updated(userId, userTimestamps :+ now), true)
              }
              _ <-
                if !allowed then
                  logger.warn(
                    s"[FrequencyCapper] User $userId exceeded marketing limit of ${config.maxMarketingPerWindow} per ${config.windowDuration}"
                  )
                else Async[F].unit
            yield allowed
