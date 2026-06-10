# ==========================================
# Stage 1: Build Flink Java Application
# ==========================================
FROM maven:3.8.8-eclipse-temurin-11 AS builder

WORKDIR /app

# Copy pom.xml and download dependencies (cached layer)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code and build fat jar
COPY src ./src
RUN mvn clean package -DskipTests

# ==========================================
# Stage 2: Flink Job Submitter
# ==========================================
FROM flink:1.18.1-java11

WORKDIR /opt/flink/usrlib

# Copy the compiled fat jar to Flink's user library directory
COPY --from=builder /app/target/real-time-feature-pipeline-1.0-SNAPSHOT.jar ./flink-job.jar

# Entrypoint script to submit the job to Flink JobManager
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "-c", "com.pipeline.FeaturePipeline", "./flink-job.jar"]
