# Flink ClickHouse Connector Example

A comprehensive example demonstrating how to use Apache Flink with ClickHouse to process and store COVID-19 epidemiological data.

## 📋 Prerequisites

- **Java 11 or higher**
- **Apache Maven 3.6+** or **Gradle 7.0+**
- **ClickHouse instance** (local or cloud)
- **Internet connection** for downloads
- **Operating System**: Linux, macOS, or Windows with WSL

## 🚀 Quick Start

### 1. Download Apache Flink

```bash
# Download Flink 2.0.0
wget https://dlcdn.apache.org/flink/flink-2.0.0/flink-2.0.0-bin-scala_2.12.tgz

# Alternative with curl (shows progress)
curl -L -# -O https://dlcdn.apache.org/flink/flink-2.0.0/flink-2.0.0-bin-scala_2.12.tgz

# Verify download (optional)
sha512sum flink-2.0.0-bin-scala_2.12.tgz
```

### 2. Extract and Start Flink

```bash
# Extract Flink
tar -xzf flink-2.0.0-bin-scala_2.12.tgz
cd flink-2.0.0

# Start Flink cluster
./bin/start-cluster.sh

# Verify Flink is running
./bin/flink list
# Or check web UI: http://localhost:8081
```

### 3. Build the Connector and Application

```bash
# Build connector (run from connector root directory)
./gradlew publishToMavenLocal

# Verify connector was published
ls ~/.m2/repository/org/apache/flink/connector/clickhouse/

# Build the application (run from maven example folder)
cd examples/maven  # adjust path as needed
mvn clean package -DskipTests

# Verify JAR was created
ls target/covid-1.0-SNAPSHOT.jar
```

## 🗄️ ClickHouse Setup

### Option A: Docker (Recommended for testing)

```bash
# Start ClickHouse with Docker
docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server

# Wait for startup
sleep 10

# Test connection
curl http://localhost:8123/ping
```

### Option B: ClickHouse Cloud

1. Go to [ClickHouse Cloud](https://clickhouse.com/)
2. Create a new service
3. Note down your connection details

### Create Database Table

```sql
-- Connect to ClickHouse
clickhouse-client  # for local install
-- or use web interface: http://localhost:8123/play

-- Create table
CREATE TABLE IF NOT EXISTS `default`.`covid` (
    date Date,
    location_key LowCardinality(String),
    new_confirmed Int32,
    new_deceased Int32,
    new_recovered Int32,
    new_tested Int32,
    cumulative_confirmed Int32,
    cumulative_deceased Int32,
    cumulative_recovered Int32,
    cumulative_tested Int32
) ENGINE = MergeTree
ORDER BY (location_key, date);
```

## 📊 Download Sample Data

```bash
# Download COVID-19 epidemiological data
wget https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv

# Alternative with curl
curl -L -# -o epidemiology.csv https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv

# Check file size and first few lines
ls -lh epidemiology.csv
head -5 epidemiology.csv
```

## ▶️ Run the Application

### Local ClickHouse

```bash
# Navigate to Flink directory
cd flink-2.0.0

# Run the application
./bin/flink run \
  /path/to/your/covid-1.0-SNAPSHOT.jar \
  -input "/path/to/epidemiology.csv" \
  -url "http://localhost:8123/default" \
  -username "default" \
  -password "" \
  -database "default" \
  -table "covid"
```

### ClickHouse Cloud

```bash
./bin/flink run \
  /path/to/your/covid-1.0-SNAPSHOT.jar \
  -input "/path/to/epidemiology.csv" \
  -url "jdbc:clickhouse://your-cluster.clickhouse.cloud:8443/default?ssl=true" \
  -username "your-username" \
  -password "your-password" \
  -database "default" \
  -table "covid"
```

## ✅ Verify Results

```sql
-- Check data was inserted
SELECT COUNT(*) FROM covid;

-- View sample data
SELECT * FROM covid LIMIT 10;

-- Check by country
SELECT location_key, COUNT(*) as records 
FROM covid 
GROUP BY location_key 
ORDER BY records DESC 
LIMIT 10;

-- Analyze data trends
SELECT 
    date,
    SUM(new_confirmed) as global_new_cases,
    SUM(cumulative_confirmed) as global_total_cases
FROM covid 
WHERE date >= '2020-01-01' 
GROUP BY date 
ORDER BY date 
LIMIT 20;
```

## 🔧 Configuration Options

### Application Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `-input` | Path to input CSV file | Yes | - |
| `-url` | ClickHouse URL | Yes | - |
| `-username` | ClickHouse username | Yes | - |
| `-password` | ClickHouse password | No | "" |
| `-database` | Target database name | Yes | - |
| `-table` | Target table name | Yes | - |


