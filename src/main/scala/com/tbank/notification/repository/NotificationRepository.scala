package com.tbank.notification.repository

import cats.effect.{Async, Ref}
import cats.syntax.all.*
import com.tbank.notification.domain.*
import java.time.Instant
import java.util.UUID

trait NotificationRepository[F[_]]:
  def insertIfAbsent(record: NotificationRecord): F[Either[NotificationRecord, NotificationRecord]]
  def findById(id: UUID): F[Option[NotificationRecord]]
  def findByIdempotencyKey(key: String): F[Option[NotificationRecord]]
  def updateStatus(id: UUID, status: DeliveryStatus, updatedAt: Instant): F[Unit]
  def tryAcquireLease(id: UUID, expectedUpdatedAt: Instant, newUpdatedAt: Instant): F[Boolean]
  def listByUser(userId: String, limit: Int = 50): F[List[NotificationRecord]]

object InMemoryNotificationRepository:

  private final case class State(
      byId: Map[UUID, NotificationRecord] = Map.empty,
      byKey: Map[String, UUID] = Map.empty,
      byUser: Map[String, Vector[UUID]] = Map.empty
  )

  def make[F[_]: Async]: F[NotificationRepository[F]] =
    Ref.of[F, State](State()).map { storage =>
      new NotificationRepository[F]:

        override def insertIfAbsent(record: NotificationRecord): F[Either[NotificationRecord, NotificationRecord]] =
          storage.modify { state =>
            state.byKey.get(record.idempotencyKey) match
              case Some(existingId) =>
                val existingRecord = state.byId(existingId)
                (state, Left(existingRecord))
              case None =>
                val updatedState = state.copy(
                  byId = state.byId.updated(record.id, record),
                  byKey = state.byKey.updated(record.idempotencyKey, record.id),
                  byUser = state.byUser.updated(
                    record.userId,
                    state.byUser.getOrElse(record.userId, Vector.empty) :+ record.id
                  )
                )
                (updatedState, Right(record))
          }

        override def findById(id: UUID): F[Option[NotificationRecord]] =
          storage.get.map(_.byId.get(id))

        override def findByIdempotencyKey(key: String): F[Option[NotificationRecord]] =
          storage.get.map { state =>
            state.byKey.get(key).flatMap(state.byId.get)
          }

        override def updateStatus(id: UUID, status: DeliveryStatus, updatedAt: Instant): F[Unit] =
          storage.update { state =>
            state.byId.get(id) match
              case Some(existing) =>
                state.copy(byId = state.byId.updated(id, existing.copy(status = status, updatedAt = updatedAt)))
              case None =>
                state
          }

        override def tryAcquireLease(id: UUID, expectedUpdatedAt: Instant, newUpdatedAt: Instant): F[Boolean] =
          storage.modify { state =>
            state.byId.get(id) match
              case Some(existing) if existing.updatedAt == expectedUpdatedAt && existing.status == DeliveryStatus.InProgress =>
                val updated = existing.copy(updatedAt = newUpdatedAt)
                (state.copy(byId = state.byId.updated(id, updated)), true)
              case _ =>
                (state, false)
          }

        override def listByUser(userId: String, limit: Int = 50): F[List[NotificationRecord]] =
          storage.get.map { state =>
            val userIds = state.byUser.getOrElse(userId, Vector.empty).takeRight(limit)
            userIds.flatMap(state.byId.get).toList.sortBy(_.createdAt)(Ordering[Instant].reverse)
          }
    }
