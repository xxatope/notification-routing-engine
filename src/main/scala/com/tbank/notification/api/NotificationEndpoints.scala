package com.tbank.notification.api

import com.tbank.notification.domain.*
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.json.circe.*
import java.util.UUID

object NotificationEndpoints:

  val healthEndpoint = endpoint.get
    .in("health" / "live")
    .summary("Liveness probe")
    .out(statusCode(StatusCode.Ok).and(stringBody))

  val readyEndpoint = endpoint.get
    .in("health" / "ready")
    .summary("Readiness probe")
    .out(statusCode(StatusCode.Ok).and(stringBody))

  private val secureBaseEndpoint = endpoint
    .in("api" / "v1" / "notifications")
    .in(header[Option[String]]("X-API-Key").description("API Authorization Key"))
    .errorOut(
      oneOf[AppError](
        oneOfVariant(StatusCode.Unauthorized, jsonBody[AppError.Unauthorized].description("Authentication failed")),
        oneOfVariant(StatusCode.NotFound, jsonBody[AppError.NotFound].description("Notification not found")),
        oneOfVariant(StatusCode.Conflict, jsonBody[AppError.AlreadyExists].description("Duplicate request")),
        oneOfVariant(StatusCode.TooManyRequests, jsonBody[AppError.RateLimitExceeded].description("Marketing rate limit reached")),
        oneOfVariant(StatusCode.BadGateway, jsonBody[AppError.DeliveryFailed].description("Delivery failed across all channels")),
        oneOfVariant(StatusCode.BadRequest, jsonBody[AppError.ValidationError].description("Validation error")),
        oneOfVariant(StatusCode.InternalServerError, jsonBody[AppError.InternalError].description("Internal server error"))
      )
    )

  val sendNotificationEndpoint = secureBaseEndpoint.post
    .summary("Send or queue a notification")
    .description("Sends a notification with cascade fallback and anti-spam check. Returns 502 Bad Gateway if all channels fail.")
    .in(jsonBody[NotificationRequest])
    .out(statusCode(StatusCode.Ok).and(jsonBody[NotificationResponse]))

  val getNotificationEndpoint = secureBaseEndpoint.get
    .summary("Get notification details by ID")
    .in(path[UUID]("id").description("Notification UUID"))
    .out(statusCode(StatusCode.Ok).and(jsonBody[NotificationResponse]))

  val getUserNotificationsEndpoint = secureBaseEndpoint.get
    .summary("List notification history for a user")
    .in("user" / path[String]("userId").description("User ID"))
    .in(query[Option[Int]]("limit").description("Max results (1-100, default 50)"))
    .out(statusCode(StatusCode.Ok).and(jsonBody[List[NotificationResponse]]))
