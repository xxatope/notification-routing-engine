package com.tbank.notification.repository

import cats.effect.{Async, Ref}
import cats.syntax.all.*
import com.tbank.notification.domain.*
import java.time.Instant
import java.util.UUID

trait NotificationRepository[F[_]]:
  def insert(record: NotificationRecord): F[Unit]
  def findById(id: UUID): F[Option[NotificationRecord]]
  def findByIdempotencyKey(key: String): F[Option[NotificationRecord]]
  def updateStatus(id: UUID, status: DeliveryStatus, updatedAt: Instant): F[Unit]
  def listByUser(userId: String): F[List[NotificationRecord]]

object InMemoryNotificationRepository:
  def make[F[_]: Async]: F[NotificationRepository[F]] =
    Ref.of[F, Map[UUID, NotificationRecord]](Map.empty).map { storage =>
      new NotificationRepository[F]:
        override def insert(record: NotificationRecord): F[Unit] =
          storage.update(_.updated(record.id, record))

        override def findById(id: UUID): F[Option[NotificationRecord]] =
          storage.get.map(_.get(id))

        override def findByIdempotencyKey(key: String): F[Option[NotificationRecord]] =
          storage.get.map(_.values.find(_.idempotencyKey == key))

        override def updateStatus(id: UUID, status: DeliveryStatus, updatedAt: Instant): F[Unit] =
          storage.update { map =>
            map.get(id) match
              case Some(existing) => map.updated(id, existing.copy(status = status, updatedAt = updatedAt))
              case None           => map
          }

        override def listByUser(userId: String): F[List[NotificationRecord]] =
          storage.get.map(_.values.filter(_.userId == userId).toList.sortBy(_.createdAt)(Ordering[Instant].reverse))
    }
