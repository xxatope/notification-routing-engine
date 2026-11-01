package com.tbank.notification.kafka

import cats.effect.Async
import cats.syntax.all.*
import com.tbank.notification.config.KafkaConfig
import com.tbank.notification.domain.NotificationRequest
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
      maxConcurrent: Int = 16
  ): KafkaConsumerWorker[F] =
    val logger = LoggerFactory[F].getLogger

    val consumerSettings =
      ConsumerSettings[F, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.consumerGroup)

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
                  engine
                    .submitNotification(req)
                    .flatMap {
                      case Right(record) =>
                        logger.info(s"Processed notification: ${record.id} with status ${record.status}") *>
                          Async[F].pure(committable.offset)
                      case Left(err) =>
                        logger.warn(s"Notification rejected: ${err.message}") *>
                          Async[F].pure(committable.offset)
                    }
                    .handleErrorWith { ex =>
                      logger.error(s"Transient error processing notification: ${ex.getMessage}") *>
                        Async[F].pure(committable.offset)
                    }
                case Left(parseErr) =>
                  logger.error(s"Failed to decode Kafka payload (malformed JSON): ${parseErr.getMessage}") *>
                    Async[F].pure(committable.offset)
            }
            .through(commitBatchWithin(batchSize = 50, batchTimeout = 1.second))
