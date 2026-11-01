ThisBuild / scalaVersion := "3.3.3"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.tbank"

val catsEffectVersion = "3.5.4"
val http4sVersion     = "0.23.26"
val tapirVersion      = "1.10.6"
val circeVersion      = "0.14.7"
val skunkVersion      = "0.6.3"
val cirisVersion      = "3.6.0"
val log4catsVersion   = "2.6.0"
val logbackVersion    = "1.5.6"
val munitCeVersion    = "2.0.0"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "notification-routing-engine",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked"
    ),
    libraryDependencies ++= Seq(
      "org.typelevel"               %% "cats-effect"             % catsEffectVersion,
      "co.fs2"                      %% "fs2-core"                % "3.10.2",
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server"     % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe"        % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % tapirVersion,
      "org.http4s"                  %% "http4s-ember-server"     % http4sVersion,
      "io.circe"                    %% "circe-core"              % circeVersion,
      "io.circe"                    %% "circe-generic"           % circeVersion,
      "io.circe"                    %% "circe-parser"            % circeVersion,
      "org.tpolecat"                %% "skunk-core"              % skunkVersion,
      "is.cir"                      %% "ciris"                   % cirisVersion,
      "org.typelevel"               %% "log4cats-slf4j"          % log4catsVersion,
      "ch.qos.logback"              %  "logback-classic"         % logbackVersion,
      "org.typelevel"               %% "munit-cats-effect"       % munitCeVersion % Test
    )
  )
