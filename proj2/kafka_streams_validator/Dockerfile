FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17
WORKDIR /app
COPY --from=build /app/target/kafka-streams-validator-1.0-SNAPSHOT.jar app.jar
CMD ["java", "-jar", "app.jar"]
