package com.tbank.notification.api

import cats.effect.Async
import cats.syntax.all.*
import com.tbank.notification.engine.RoutingEngine
import org.http4s.HttpRoutes
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter

object NotificationRoutes:

  def make[F[_]: Async](engine: RoutingEngine[F]): HttpRoutes[F] =
    val healthServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.healthEndpoint.serverLogicSuccess(_ => Async[F].pure("UP"))

    val readyServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.readyEndpoint.serverLogicSuccess(_ => Async[F].pure("READY"))

    val sendServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.sendNotificationEndpoint.serverLogic(req => engine.submitNotification(req))

    val getNotificationServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.getNotificationEndpoint.serverLogic(id => engine.getNotification(id))

    val getUserNotificationsServerEndpoint: ServerEndpoint[Any, F] =
      NotificationEndpoints.getUserNotificationsEndpoint.serverLogic(userId =>
        engine.getNotificationsForUser(userId).map(Right(_))
      )

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
