package com.tbank.notification.engine

import cats.effect.{Async, Ref}
import cats.syntax.all.*
import com.tbank.notification.domain.Priority
import org.typelevel.log4cats.LoggerFactory
import java.time.Instant
import scala.concurrent.duration.*

trait FrequencyCapper[F[_]]:
  def maxLimit: Int
  def isAllowed(userId: String, priority: Priority): F[Boolean]
  def refund(userId: String, priority: Priority): F[Unit]

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

      override def maxLimit: Int = config.maxMarketingPerWindow

      override def isAllowed(userId: String, priority: Priority): F[Boolean] =
        priority match
          case Priority.Critical =>
            Async[F].pure(true)

          case Priority.Marketing =>
            for
              now <- Async[F].realTimeInstant
              cutoff = now.minusMillis(config.windowDuration.toMillis)
              allowed <- state.modify { map =>
                val activeTimestamps = map.getOrElse(userId, Vector.empty).filter(_.isAfter(cutoff))
                if activeTimestamps.size >= config.maxMarketingPerWindow then
                  (map.updated(userId, activeTimestamps), false)
                else
                  (map.updated(userId, activeTimestamps :+ now), true)
              }
              _ <-
                if !allowed then
                  logger.debug(s"User $userId reached rate limit ($config.maxMarketingPerWindow)")
                else Async[F].unit
            yield allowed

      override def refund(userId: String, priority: Priority): F[Unit] =
        priority match
          case Priority.Critical => Async[F].unit
          case Priority.Marketing =>
            state.update { map =>
              map.get(userId) match
                case Some(timestamps) if timestamps.nonEmpty =>
                  map.updated(userId, timestamps.dropRight(1))
                case _ =>
                  map
            }
