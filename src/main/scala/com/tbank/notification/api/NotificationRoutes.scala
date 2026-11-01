package com.tbank.notification.api

import cats.effect.Async
import cats.syntax.all.*
import com.tbank.notification.domain.{AppError, NotificationResponse}
import com.tbank.notification.engine.RoutingEngine
import org.http4s.HttpRoutes
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter

object NotificationRoutes:

  def make[F[_]: Async](engine: RoutingEngine[F], validApiKey: String): HttpRoutes[F] =

    def authenticate(apiKeyOpt: Option[String]): F[Either[AppError, Unit]] =
      apiKeyOpt match
        case Some(key) if key == validApiKey =>
          Async[F].pure(Right(()))
        case _ =>
          Async[F].pure(Left(AppError.Unauthorized()))

    val healthServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.healthEndpoint.serverLogicSuccess(_ => Async[F].pure("UP"))

    val readyServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.readyEndpoint.serverLogicSuccess(_ => Async[F].pure("READY"))

    val sendServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.sendNotificationEndpoint.serverLogic { case (apiKeyOpt, req) =>
        authenticate(apiKeyOpt).flatMap {
          case Left(err) => Async[F].pure(Left(err))
          case Right(_) =>
            engine.submitNotification(req).map(_.map(NotificationResponse.fromRecord))
        }
      }

    val getNotificationServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.getNotificationEndpoint.serverLogic { case (apiKeyOpt, id) =>
        authenticate(apiKeyOpt).flatMap {
          case Left(err) => Async[F].pure(Left(err))
          case Right(_) =>
            engine.getNotification(id).map(_.map(NotificationResponse.fromRecord))
        }
      }

    val getUserNotificationsServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.getUserNotificationsEndpoint.serverLogic { case (apiKeyOpt, userId, limitOpt) =>
        authenticate(apiKeyOpt).flatMap {
          case Left(err) => Async[F].pure(Left(err))
          case Right(_) =>
            val limit = limitOpt.getOrElse(50).min(100).max(1)
            engine
              .getNotificationsForUser(userId, limit)
              .map(records => Right(records.map(NotificationResponse.fromRecord)))
        }
      }

    val apiEndpoints: List[ServerEndpoint[Any, F]] = List(
      healthServerEndpoint,
      readyServerEndpoint,
      sendServerEndpoint,
      getNotificationServerEndpoint,
      getUserNotificationsServerEndpoint
    )

    val swaggerEndpoints: List[ServerEndpoint[Any, F]] =
      SwaggerInterpreter().fromServerEndpoints[F](
        apiEndpoints,
        title = "Notification & Routing Engine API",
        version = "1.0.0"
      )

    Http4sServerInterpreter[F]().toRoutes(apiEndpoints ++ swaggerEndpoints)
