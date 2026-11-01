package com.tbank.notification.config

import cats.effect.Async
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port, host, port}

final case class HttpServerConfig(
    host: Host,
    port: Port
)

final case class KafkaConfig(
    bootstrapServers: String,
    consumerGroup: String,
    inputTopic: String,
    dlqTopic: String,
    enabled: Boolean
)

final case class AppConfig(
    httpServer: HttpServerConfig,
    kafka: KafkaConfig
)

object AppConfig:
  def load[F[_]: Async]: F[AppConfig] =
    val config = (
      env("HTTP_HOST").as[String].default("0.0.0.0"),
      env("HTTP_PORT").as[Int].default(8081),
      env("KAFKA_BOOTSTRAP_SERVERS").as[String].default("localhost:9092"),
      env("KAFKA_CONSUMER_GROUP").as[String].default("notification-engine-group"),
      env("KAFKA_INPUT_TOPIC").as[String].default("notifications.requests"),
      env("KAFKA_DLQ_TOPIC").as[String].default("notifications.dlq"),
      env("KAFKA_ENABLED").as[Boolean].default(false)
    ).parMapN { (hostStr, portInt, bootstrap, group, inTopic, dlq, kafkaEnabled) =>
      AppConfig(
        httpServer = HttpServerConfig(
          host = Host.fromString(hostStr).getOrElse(host"0.0.0.0"),
          port = Port.fromInt(portInt).getOrElse(port"8081")
        ),
        kafka = KafkaConfig(
          bootstrapServers = bootstrap,
          consumerGroup = group,
          inputTopic = inTopic,
          dlqTopic = dlq,
          enabled = kafkaEnabled
        )
      )
    }

    config.load[F]
