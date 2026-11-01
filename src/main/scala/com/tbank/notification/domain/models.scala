package com.tbank.notification.domain

import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.*
import sttp.tapir.Schema
import java.time.Instant
import java.util.UUID

enum Priority:
  case Critical
  case Marketing

object Priority:
  given Codec[Priority] = Codec.from(
    Decoder.decodeString.emap {
      case "CRITICAL"  => Right(Priority.Critical)
      case "MARKETING" => Right(Priority.Marketing)
      case other       => Left(s"Unknown priority: $other")
    },
    Encoder.encodeString.contramap {
      case Priority.Critical  => "CRITICAL"
      case Priority.Marketing => "MARKETING"
    }
  )
  given Schema[Priority] = Schema.string[Priority]

enum Channel:
  case Push
  case Sms

object Channel:
  given Codec[Channel] = Codec.from(
    Decoder.decodeString.emap {
      case "PUSH" => Right(Channel.Push)
      case "SMS"  => Right(Channel.Sms)
      case other  => Left(s"Unknown channel: $other")
    },
    Encoder.encodeString.contramap {
      case Channel.Push => "PUSH"
      case Channel.Sms  => "SMS"
    }
  )
  given Schema[Channel] = Schema.string[Channel]

enum DeliveryStatus:
  case Pending
  case InProgress
  case Delivered(channel: Channel, provider: String, deliveredAt: Instant)
  case Failed(reason: String)
  case RateLimited

object DeliveryStatus:
  given Codec[DeliveryStatus] = deriveCodec[DeliveryStatus]
  given Schema[DeliveryStatus] = Schema.derived[DeliveryStatus]

enum SendResult:
  case Success(provider: String)
  case TransientFailure(reason: String)
  case PermanentFailure(reason: String)

final case class NotificationRequest(
    idempotencyKey: String,
    userId: String,
    priority: Priority,
    content: String,
    phone: Option[String] = None,
    deviceToken: Option[String] = None
):
  def validate: Either[String, Unit] =
    if idempotencyKey.trim.isEmpty || idempotencyKey.length > 128 then
      Left("idempotencyKey must be between 1 and 128 characters")
    else if userId.trim.isEmpty then
      Left("userId must not be empty")
    else if content.trim.isEmpty then
      Left("content must not be empty")
    else if phone.isEmpty && deviceToken.isEmpty then
      Left("At least one delivery destination (phone or deviceToken) must be specified")
    else
      Right(())

object NotificationRequest:
  given Codec[NotificationRequest] = deriveCodec[NotificationRequest]
  given Schema[NotificationRequest] = Schema.derived[NotificationRequest]

// Хранилище (внутренняя модель с полными данными)
final case class NotificationRecord(
    id: UUID,
    idempotencyKey: String,
    userId: String,
    priority: Priority,
    content: String,
    phone: Option[String],
    deviceToken: Option[String],
    status: DeliveryStatus,
    createdAt: Instant,
    updatedAt: Instant
)

object NotificationRecord:
  given Codec[NotificationRecord] = deriveCodec[NotificationRecord]
  given Schema[NotificationRecord] = Schema.derived[NotificationRecord]

// Публичный ответ API (БЕЗ раскрытия OTP content и deviceToken)
final case class NotificationResponse(
    id: UUID,
    idempotencyKey: String,
    userId: String,
    priority: Priority,
    maskedPhone: Option[String],
    status: DeliveryStatus,
    createdAt: Instant,
    updatedAt: Instant
)

object NotificationResponse:
  def fromRecord(record: NotificationRecord): NotificationResponse =
    NotificationResponse(
      id = record.id,
      idempotencyKey = record.idempotencyKey,
      userId = record.userId,
      priority = record.priority,
      maskedPhone = record.phone.map(maskPhone),
      status = record.status,
      createdAt = record.createdAt,
      updatedAt = record.updatedAt
    )

  private def maskPhone(phone: String): String =
    if phone.length >= 7 then
      phone.take(4) + "****" + phone.takeRight(2)
    else
      "***"

  given Codec[NotificationResponse] = deriveCodec[NotificationResponse]
  given Schema[NotificationResponse] = Schema.derived[NotificationResponse]
