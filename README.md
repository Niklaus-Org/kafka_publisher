# Kafka Publisher - Driver Location Service

A Spring Boot application that publishes real-time driver location updates to Apache Kafka. This service acts as a Kafka producer, sending driver location data (coordinates and location details) to a Kafka topic for consumption by other microservices.

## 📋 Project Overview

This is a **Kafka Publisher** application built with Spring Boot 4.0.0 and Java 17. It provides REST APIs to receive driver location information and publishes it to Kafka topics for real-time data streaming.

### Use Case
- Track driver locations in real-time
- Send location updates (latitude, longitude, location name) to Kafka
- Enable other services to consume and process location data

## 🏗️ Architecture

### Components

```
┌─────────────────────────────────────────┐
│           REST Client/API                │
└────────────────┬────────────────────────┘
                 │ POST /kafka/publish
                 ↓
        ┌─────────────────────┐
        │  KafkaController    │
        └──────────┬──────────┘
                   │ (receives DriverLocation)
                   ↓
        ┌──────────────────────────┐
        │ KafkPublisherService     │
        │ (publishes to Kafka)     │
        └──────────┬───────────────┘
                   │
                   ↓
        ┌──────────────────────────┐
        │  KafkaConfiguration      │
        │  (ProducerFactory,       │
        │   KafkaTemplate)         │
        └──────────┬───────────────┘
                   │
                   ↓
        ┌──────────────────────────┐
        │  Apache Kafka            │
        │  (Topic: driver-location)│
        └──────────────────────────┘
```

## 📁 Project Structure

```
kafkalearning/
├── src/
│   ├── main/
│   │   ├── java/com/niketan/kafka/kafkalearning/
│   │   │   ├── KafkalearningApplication.java      # Spring Boot entry point
│   │   │   ├── config/
│   │   │   │   └── KafkaConfiguration.java        # Kafka producer configuration
│   │   │   ├── controller/
│   │   │   │   └── KafkaController.java           # REST API endpoints
│   │   │   ├── model/
│   │   │   │   └── DriverLocation.java            # Data model for driver location
│   │   │   └── service/
│   │   │       └── KafkPublisherService.java      # Service to publish to Kafka
│   │   └── resources/
│   │       └── application.properties             # Configuration properties
│   └── test/
│       └── java/com/niketan/kafka/kafkalearning/
│           └── KafkalearningApplicationTests.java
├── pom.xml                                         # Maven dependencies
└── README.md                                       # This file
```

## 🔧 Key Components

### 1. **KafkaConfiguration** (`config/KafkaConfiguration.java`)
Configures the Kafka producer factory and KafkaTemplate bean:
- Sets up **ProducerFactory** with Kafka bootstrap servers
- Configures **key serializer** (StringSerializer)
- Configures **value serializer** (JsonSerializer for objects)
- Provides **KafkaTemplate** for sending messages

### 2. **DriverLocation Model** (`model/DriverLocation.java`)
Data model representing a driver's location:
- `driverId` - Unique identifier for the driver
- `latitude` - Driver's latitude coordinate
- `longitude` - Driver's longitude coordinate
- `location` - Human-readable location name

### 3. **KafkaController** (`controller/KafkaController.java`)
REST API endpoint:
- **POST** `/kafka/publish` - Accepts driver location JSON and publishes to Kafka

### 4. **KafkPublisherService** (`service/KafkPublisherService.java`)
Service layer that handles Kafka publishing:
- Publishes DriverLocation objects to Kafka topic
- Uses driver ID as the message key
- Includes error handling for failed publishes

## 🚀 Getting Started

### Prerequisites
- Java 17 or higher
- Maven 3.6+
- Apache Kafka 2.8+ running locally or remotely
- Spring Boot 4.0.0

### Installation & Setup

1. **Clone/Download the project**
   ```bash
   cd kafkalearning
   ```

2. **Ensure Kafka is running**
   ```bash
   # Make sure Kafka broker is accessible at localhost:9092
   ```

3. **Build the project**
   ```bash
   mvn clean install
   ```

4. **Run the application**
   ```bash
   mvn spring-boot:run
   ```
   
   The application will start on **http://localhost:8081**

## ⚙️ Configuration

Edit `src/main/resources/application.properties` to configure:

```properties
# Server port (default: 8081)
server.port=8081

# Kafka Bootstrap Servers
spring.kafka.producer.bootstrap-servers=localhost:9092

# Key Serializer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer

# Value Serializer (JSON format)
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Kafka Topic for driver locations
kafka.topic.driver-location=driver-location-updates
```

## 📡 API Usage

### Publish Driver Location

**Endpoint:** `POST /kafka/publish`

**Request Body:**
```json
{
  "driverId": "driver123",
  "latitude": 40.7128,
  "longitude": -74.0060,
  "location": "New York City"
}
```

**Response:**
```
Published Successfully
```

**Example using cURL:**
```bash
curl -X POST http://localhost:8081/kafka/publish \
  -H "Content-Type: application/json" \
  -d '{
    "driverId": "driver001",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "location": "San Francisco"
  }'
```

**Example using Python:**
```python
import requests
import json

url = "http://localhost:8081/kafka/publish"
payload = {
    "driverId": "driver002",
    "latitude": 34.0522,
    "longitude": -118.2437,
    "location": "Los Angeles"
}

response = requests.post(url, json=payload)
print(response.text)
```

## 📦 Dependencies

Key Maven dependencies included:

- **spring-boot-starter-kafka** - Kafka support for Spring Boot
- **spring-boot-starter-webmvc** - REST API support
- **jackson-databind** - JSON serialization/deserialization
- **lombok** - Reduce boilerplate code (@Getter, @Setter, etc.)

## 🔍 How It Works

1. **User sends HTTP POST request** with driver location data
2. **KafkaController receives** the DriverLocation object
3. **KafkPublisherService.publishDriverLocation()** is called
4. **KafkaTemplate.send()** publishes the message to Kafka topic with driver ID as key
5. **Kafka broker** receives and stores the message
6. **Consumer applications** can subscribe to the topic and process the data

## 🐛 Debugging & Logs

The application includes console logs to help with debugging:
- Logs when location is received
- Logs when location is published to Kafka
- Logs any exceptions during publishing

Check the console output for messages like:
```
Received Driver Location: DriverLocation(driverId=driver123, latitude=40.7128, longitude=-74.0060, location=New York City)
Published Driver Location to Kafka: DriverLocation(driverId=driver123, latitude=40.7128, longitude=-74.0060, location=New York City)
```

## 🚀 Production Considerations

- **Error Handling**: Add proper exception handling and retry logic
- **Logging**: Implement proper logging framework (SLF4J/Log4j)
- **Monitoring**: Add metrics and health checks
- **Partitioning**: Configure topic partitions based on throughput requirements
- **Compression**: Enable message compression to reduce network traffic
- **Security**: Add authentication and TLS/SSL for Kafka connections
- **Load Balancing**: Use multiple producer instances for high throughput

## 📚 Additional Resources

- [Spring Boot Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Boot Reference](https://spring.io/projects/spring-boot)

## 📝 License

This is a learning project. Modify as needed for your use case.

## 👨‍💻 Author

Niketan
