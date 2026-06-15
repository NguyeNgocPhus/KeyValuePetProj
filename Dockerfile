# Stage 1: Build stage
FROM maven:3.9.9-eclipse-temurin-25-alpine AS builder
WORKDIR /app
COPY pom.xml .
# Cache maven dependencies
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Runtime stage
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/distributed_system-shaded.jar ./app.jar

# Setup persistent directory for WAL logs
RUN mkdir -p /app/data && chmod -R 777 /app/data
VOLUME /app/data

# Enable Java preview features
ENV JAVA_TOOL_OPTIONS="--enable-preview"

ENTRYPOINT ["java", "-jar", "app.jar"]
