package com.tbank.notification

import cats.effect.{IO, IOApp}
import com.tbank.notification.api.NotificationRoutes
import com.tbank.notification.config.AppConfig
import com.tbank.notification.engine.{InMemoryFrequencyCapper, RoutingEngine}
import com.tbank.notification.kafka.KafkaConsumerWorker
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
      _      <- logger.info("Initializing Notification Routing Engine...")
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

      httpApp = NotificationRoutes.make[IO](engine, config.apiKey).orNotFound
      loggedApp = HttpLogger.httpApp(logHeaders = true, logBody = false)(httpApp)

      kafkaWorker = KafkaConsumerWorker.make[IO](config.kafka, engine)

      serverResource = EmberServerBuilder
        .default[IO]
        .withHost(config.httpServer.host)
        .withPort(config.httpServer.port)
        .withHttpApp(loggedApp)
        .build
        .use { server =>
          logger.info(s"Server successfully bound to http://${server.address.getHostString}:${server.address.getPort}") *>
            logger.info(s"Interactive Swagger UI: http://localhost:${server.address.getPort}/docs") *>
            IO.never
        }

      // Запуск сервера и фонового воркера вместе (both)
      _ <- (serverResource, kafkaWorker.run.compile.drain).parTupled.void
    yield ()
