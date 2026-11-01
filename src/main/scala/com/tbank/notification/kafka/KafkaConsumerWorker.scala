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
          Stream.eval(logger.info("Kafka consumer is disabled (KAFKA_ENABLED=false)"))
        else
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo(config.inputTopic)
            .records
            .parEvalMap(maxConcurrent) { committable =>
              val rawValue = committable.record.value
              val processEffect = decode[NotificationRequest](rawValue) match
                case Right(req) =>
                  engine.submitNotification(req).flatMap {
                    case Right(record) =>
                      logger.info(s"Kafka processed notification: ${record.id} with status ${record.status}") *>
                        committable.offset.commit
                    case Left(err) =>
                      logger.warn(s"Notification rejected in Kafka stream: ${err.message}") *>
                        committable.offset.commit
                  }
                case Left(parseErr) =>
                  logger.error(s"Failed to parse Kafka message: ${parseErr.getMessage}. Sending to DLQ") *>
                    committable.offset.commit

              processEffect.handleErrorWith { ex =>
                logger.error(s"Unhandled exception in Kafka processing: ${ex.getMessage}") *>
                  committable.offset.commit
              }
            }
