package com.tbank.notification.config

import cats.effect.Async
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port}

final case class HttpServerConfig(
    host: Host,
    port: Port
)

final case class KafkaConfig(
    bootstrapServers: String,
    consumerGroup: String,
    inputTopic: String,
    enabled: Boolean
)

final case class AppConfig(
    httpServer: HttpServerConfig,
    kafka: KafkaConfig,
    apiKey: Secret[String]
)

object AppConfig:

  private val MinApiKeyLength = 16

  def load[F[_]: Async]: F[AppConfig] =
    val config = (
      env("HTTP_HOST").as[String].default("0.0.0.0"),
      env("HTTP_PORT").as[Int].default(8081),
      env("KAFKA_BOOTSTRAP_SERVERS").as[String].default("localhost:9092"),
      env("KAFKA_CONSUMER_GROUP").as[String].default("notification-engine-group"),
      env("KAFKA_INPUT_TOPIC").as[String].default("notifications.requests"),
      env("KAFKA_ENABLED").as[Boolean].default(false),
      env("APP_API_KEY").secret
    ).parMapN { (hostStr, portInt, bootstrap, group, inTopic, kafkaEnabled, key) =>
      (hostStr, portInt, bootstrap, group, inTopic, kafkaEnabled, key)
    }

    config.load[F].flatMap { case (hostStr, portInt, bootstrap, group, inTopic, kafkaEnabled, key) =>
      for
        h <- Host.fromString(hostStr) match
          case Some(parsedHost) => Async[F].pure(parsedHost)
          case None =>
            Async[F].raiseError[Host](new IllegalArgumentException(s"Invalid HTTP_HOST configuration: '$hostStr'"))

        p <- Port.fromInt(portInt) match
          case Some(parsedPort) => Async[F].pure(parsedPort)
          case None =>
            Async[F].raiseError[Port](new IllegalArgumentException(s"Invalid HTTP_PORT configuration: '$portInt'"))

        _ <-
          if key.value.length < MinApiKeyLength then
            Async[F].raiseError[Unit](
              new IllegalArgumentException(s"APP_API_KEY must be at least $MinApiKeyLength characters long")
            )
          else Async[F].unit
      yield AppConfig(
        httpServer = HttpServerConfig(h, p),
        kafka = KafkaConfig(
          bootstrapServers = bootstrap,
          consumerGroup = group,
          inputTopic = inTopic,
          enabled = kafkaEnabled
        ),
        apiKey = key
      )
    }
