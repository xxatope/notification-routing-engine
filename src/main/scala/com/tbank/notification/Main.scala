package com.tbank.notification

import cats.effect.{IO, IOApp}
import com.tbank.notification.api.NotificationRoutes
import com.tbank.notification.config.AppConfig
import com.tbank.notification.engine.{InMemoryFrequencyCapper, RoutingEngine}
import com.tbank.notification.provider.{MockPushProvider, MockSmsProvider}
import com.tbank.notification.repository.InMemoryNotificationRepository
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.Logger as HttpLogger
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

object Main extends IOApp.Simple:

  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  override def run: IO[Unit] =
    val logger = LoggerFactory[IO].getLogger

    for
      _      <- logger.info("Starting Notification Routing Engine...")
      config <- AppConfig.load[IO]

      repo            <- InMemoryNotificationRepository.make[IO]
      frequencyCapper <- InMemoryFrequencyCapper.make[IO]()

      pushProvider = MockPushProvider.make[IO]()
      smsPrimary   = MockSmsProvider.make[IO]("SMS-Alpha-Primary")
      smsSecondary = MockSmsProvider.make[IO]("SMS-Beta-Backup")

      engine = RoutingEngine.make[IO](
        pushProvider = pushProvider,
        smsPrimary = smsPrimary,
        smsSecondary = smsSecondary,
        frequencyCapper = frequencyCapper,
        repository = repo
      )

      httpApp = NotificationRoutes.make[IO](engine).orNotFound
      loggedApp = HttpLogger.httpApp(logHeaders = true, logBody = false)(httpApp)

      _ <- logger.info(s"Server started on http://${config.httpServer.host}:${config.httpServer.port}")
      _ <- logger.info(s"Swagger UI available at http://localhost:${config.httpServer.port}/docs")

      _ <- EmberServerBuilder
        .default[IO]
        .withHost(config.httpServer.host)
        .withPort(config.httpServer.port)
        .withHttpApp(loggedApp)
        .build
        .useForever
    yield ()
