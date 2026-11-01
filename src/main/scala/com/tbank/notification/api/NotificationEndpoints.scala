package com.tbank.notification.api

import com.tbank.notification.domain.*
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.json.circe.*
import java.util.UUID

object NotificationEndpoints:

  val healthEndpoint = endpoint.get
    .in("health" / "live")
    .summary("Liveness check")
    .out(statusCode(StatusCode.Ok).and(stringBody))

  val readyEndpoint = endpoint.get
    .in("health" / "ready")
    .summary("Readiness check")
    .out(statusCode(StatusCode.Ok).and(stringBody))

  private val baseEndpoint = endpoint
    .in("api" / "v1" / "notifications")
    .errorOut(
      oneOf[AppError](
        oneOfVariant(StatusCode.NotFound, jsonBody[AppError.NotFound].description("Notification not found")),
        oneOfVariant(StatusCode.Conflict, jsonBody[AppError.AlreadyExists].description("Duplicate request")),
        oneOfVariant(StatusCode.TooManyRequests, jsonBody[AppError.RateLimitExceeded].description("Marketing rate limit reached")),
        oneOfVariant(StatusCode.BadRequest, jsonBody[AppError.ValidationError].description("Validation error")),
        oneOfVariant(StatusCode.InternalServerError, jsonBody[AppError.InternalError].description("Internal server error"))
      )
    )

  val sendNotificationEndpoint = baseEndpoint.post
    .summary("Send or queue a notification")
    .description("Sends a notification with cascade fallback (Push -> SMS Alpha -> SMS Beta) and anti-spam check")
    .in(jsonBody[NotificationRequest])
    .out(statusCode(StatusCode.Ok).and(jsonBody[NotificationRecord]))

  val getNotificationEndpoint = baseEndpoint.get
    .summary("Get notification details by ID")
    .in(path[UUID]("id").description("Notification UUID"))
    .out(statusCode(StatusCode.Ok).and(jsonBody[NotificationRecord]))

  val getUserNotificationsEndpoint = baseEndpoint.get
    .summary("List notification history for a user")
    .in("user" / path[String]("userId").description("User ID"))
    .out(statusCode(StatusCode.Ok).and(jsonBody[List[NotificationRecord]]))
