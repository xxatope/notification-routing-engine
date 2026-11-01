package com.tbank.notification.domain

import io.circe.Codec
import io.circe.generic.semiauto.*
import sttp.tapir.Schema
import java.util.UUID

sealed trait AppError extends Throwable:
  def message: String
  override def getMessage: String = message

object AppError:
  final case class AlreadyExists(idempotencyKey: String) extends AppError:
    val message = s"Notification with idempotencyKey '$idempotencyKey' already exists"

  final case class NotFound(id: UUID) extends AppError:
    val message = s"Notification with id '$id' was not found"

  final case class RateLimitExceeded(userId: String, limit: Int) extends AppError:
    val message = s"Marketing frequency cap exceeded for user '$userId' (limit: $limit per window)"

  final case class ValidationError(details: String) extends AppError:
    val message = s"Validation failed: $details"

  final case class InternalError(details: String) extends AppError:
    val message = s"Internal error: $details"

  given Codec[AppError] = deriveCodec[AppError]
  given Schema[AppError] = Schema.derived[AppError]
