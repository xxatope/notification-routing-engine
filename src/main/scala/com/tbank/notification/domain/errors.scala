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

  object AlreadyExists:
    given Codec[AlreadyExists] = deriveCodec[AlreadyExists]
    given Schema[AlreadyExists] = Schema.derived[AlreadyExists]

  final case class NotFound(id: UUID) extends AppError:
    val message = s"Notification with id '$id' was not found"

  object NotFound:
    given Codec[NotFound] = deriveCodec[NotFound]
    given Schema[NotFound] = Schema.derived[NotFound]

  final case class RateLimitExceeded(userId: String, limit: Int) extends AppError:
    val message = s"Marketing frequency cap exceeded for user '$userId' (limit: $limit per window)"

  object RateLimitExceeded:
    given Codec[RateLimitExceeded] = deriveCodec[RateLimitExceeded]
    given Schema[RateLimitExceeded] = Schema.derived[RateLimitExceeded]

  final case class ValidationError(details: String) extends AppError:
    val message = s"Validation failed: $details"

  object ValidationError:
    given Codec[ValidationError] = deriveCodec[ValidationError]
    given Schema[ValidationError] = Schema.derived[ValidationError]

  final case class InternalError(details: String) extends AppError:
    val message = s"Internal error: $details"

  object InternalError:
    given Codec[InternalError] = deriveCodec[InternalError]
    given Schema[InternalError] = Schema.derived[InternalError]

  given Codec[AppError] = deriveCodec[AppError]
  given Schema[AppError] = Schema.derived[AppError]
