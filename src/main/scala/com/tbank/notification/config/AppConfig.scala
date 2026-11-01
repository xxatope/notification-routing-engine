package com.tbank.notification.config

import cats.effect.Async
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port, host, port}

final case class HttpServerConfig(
    host: Host,
    port: Port
)

final case class AppConfig(
    httpServer: HttpServerConfig
)

object AppConfig:
  def load[F[_]: Async]: F[AppConfig] =
    val config = (
      env("HTTP_HOST").as[String].default("0.0.0.0"),
      env("HTTP_PORT").as[Int].default(8080)
    ).parMapN { (h, p) =>
      AppConfig(
        httpServer = HttpServerConfig(
          host = Host.fromString(h).getOrElse(host"0.0.0.0"),
          port = Port.fromInt(p).getOrElse(port"8080")
        )
      )
    }

    config.load[F]
