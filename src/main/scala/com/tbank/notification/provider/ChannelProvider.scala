package com.tbank.notification.provider

import cats.effect.Async
import cats.syntax.all.*
import com.tbank.notification.domain.SendResult
import org.typelevel.log4cats.LoggerFactory
import scala.concurrent.duration.*

trait PushProvider[F[_]]:
  def sendPush(deviceToken: String, content: String): F[SendResult]

trait SmsProvider[F[_]]:
  def name: String
  def sendSms(phoneNumber: String, content: String): F[SendResult]

object MockPushProvider:
  def make[F[_]: Async: LoggerFactory](simulatedDelay: FiniteDuration = 200.millis): PushProvider[F] =
    val logger = LoggerFactory[F].getLogger
    new PushProvider[F]:
      override def sendPush(deviceToken: String, content: String): F[SendResult] =
        for
          _ <- logger.info(s"[PushProvider] Attempting push to token: ${deviceToken.take(10)}...")
          _ <- Async[F].sleep(simulatedDelay)
          result <-
            if deviceToken.startsWith("fail-transient") then
              logger.warn(s"[PushProvider] Simulated transient error for $deviceToken") *>
                Async[F].pure(SendResult.TransientFailure("APNS connection timeout"))
            else if deviceToken.startsWith("fail-permanent") then
              logger.error(s"[PushProvider] Simulated permanent error for $deviceToken") *>
                Async[F].pure(SendResult.PermanentFailure("Device token unregistered"))
            else
              logger.info(s"[PushProvider] Successfully delivered push to token: ${deviceToken.take(10)}...") *>
                Async[F].pure(SendResult.Success("APNS-FCM-Gateway"))
        yield result

object MockSmsProvider:
  def make[F[_]: Async: LoggerFactory](
      providerName: String,
      simulatedDelay: FiniteDuration = 300.millis,
      alwaysFail: Boolean = false
  ): SmsProvider[F] =
    val logger = LoggerFactory[F].getLogger
    new SmsProvider[F]:
      override def name: String = providerName

      override def sendSms(phoneNumber: String, content: String): F[SendResult] =
        for
          _ <- logger.info(s"[$name] Sending SMS to $phoneNumber")
          _ <- Async[F].sleep(simulatedDelay)
          result <-
            if alwaysFail || phoneNumber.contains("0000") then
              logger.warn(s"[$name] Gateway failed for phone: $phoneNumber") *>
                Async[F].pure(SendResult.TransientFailure(s"$name route degraded"))
            else
              logger.info(s"[$name] SMS delivered to $phoneNumber successfully") *>
                Async[F].pure(SendResult.Success(name))
        yield result
