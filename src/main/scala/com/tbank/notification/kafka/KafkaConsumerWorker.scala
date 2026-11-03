package com.tbank.notification.kafka

import cats.effect.Async
import cats.syntax.all.*
import com.tbank.notification.config.KafkaConfig
import com.tbank.notification.domain.{AppError, NotificationRequest}
import com.tbank.notification.engine.RoutingEngine
import fs2.Stream
import fs2.kafka.*
import io.circe.parser.decode
import org.typelevel.log4cats.LoggerFactory
import scala.concurrent.duration.*

trait KafkaConsumerWorker[F[_]]:
  def run: Stream[F, Unit]

object KafkaConsumerWorker:

  def make[F[_]: Async: LoggerFactory](
      config: KafkaConfig,
      engine: RoutingEngine[F],
      maxConcurrent: Int = 16,
      maxRetries: Int = 3
  ): KafkaConsumerWorker[F] =
    val logger = LoggerFactory[F].getLogger

    val consumerSettings =
      ConsumerSettings[F, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.consumerGroup)

    def processWithRetry(req: NotificationRequest, attemptsLeft: Int): F[Unit] =
      engine.submitNotification(req).attempt.flatMap {
        case Right(Right(record)) =>
          logger.info(s"Kafka processed notification: ${record.id} with status ${record.status}")
        case Right(Left(AppError.DeliveryFailed(reason))) =>
          logger.warn(s"Notification delivery failed: $reason (business failure, committing offset)")
        case Right(Left(err)) =>
          logger.warn(s"Notification rejected by domain logic: ${err.message} (committing offset)")
        case Left(ex) =>
          if attemptsLeft > 1 then
            val delay = (maxRetries - attemptsLeft + 1) * 500
            logger.warn(s"Transient error processing notification: ${ex.getMessage}. Retrying in ${delay}ms...") *>
              Async[F].sleep(delay.millis) *>
              processWithRetry(req, attemptsLeft - 1)
          else
            logger.error(s"Exhausted all retries for notification '${req.idempotencyKey}'. Failing message without commit.") *>
              Async[F].raiseError(ex)
      }

    new KafkaConsumerWorker[F]:
      override def run: Stream[F, Unit] =
        if !config.enabled then
          Stream.eval(logger.info("Kafka consumer disabled (KAFKA_ENABLED=false). Worker idle.")) ++ Stream.never
        else
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo(config.inputTopic)
            .records
            .parEvalMap(maxConcurrent) { committable =>
              val rawValue = committable.record.value
              decode[NotificationRequest](rawValue) match
                case Right(req) =>
                  processWithRetry(req, maxRetries).as(committable.offset)

                case Left(parseErr) =>
                  logger.error(
                    s"Malformed JSON in Kafka message (dropping poison-pill): ${parseErr.getMessage}"
                  ) *> Async[F].pure(committable.offset)
            }
            .through(commitBatchWithin[F](50, 1.second))
