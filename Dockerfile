# Multi-stage build for minimal production image footprint and maximum security

FROM sbtscala/scala-sbt:eclipse-temurin-21.0.2_13_1.9.9_3.3.3 AS builder

WORKDIR /app

# Cache dependencies
COPY project/build.properties project/plugins.sbt ./project/
COPY build.sbt ./
RUN sbt update

# Compile and package application
COPY src ./src
RUN sbt stage

# Production runtime image
FROM eclipse-temurin:21-jre-alpine AS runner

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder --chown=appuser:appgroup /app/target/universal/stage ./

USER appuser

ENV HTTP_HOST=0.0.0.0
ENV HTTP_PORT=8081

EXPOSE 8081

ENTRYPOINT ["bin/notification-routing-engine"]
