# Data Source Connectors

Data Caterer supports connecting to databases, file systems, messaging systems, and HTTP APIs for reading and writing test data.

**16 features** in this category.

## Table of Contents

- [Databases](#databases) (4 features)
- [File Formats](#files) (8 features)
- [Messaging Systems](#messaging) (3 features)
- [HTTP/REST](#http) (1 features)

## Databases

### PostgreSQL Connector

**ID**: `connector.databases.postgres`
**Status**: Stable

Connect to PostgreSQL databases for reading and writing data. Supports table-level configuration, custom queries, and JDBC options.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | JDBC connection URL YAML: `dataSources[].connection.options.url` |
| `user` | string | No | `-` | Database username YAML: `dataSources[].connection.options.user` |
| `password` | string | No | `-` | Database password YAML: `dataSources[].connection.options.password` |
| `driver` | string | No | `org.postgresql.Driver` | JDBC driver class |
| `dbtable` | string | No | `-` | Target table (schema.table) YAML: `dataSources[].steps[].options.dbtable` |
| `query` | string | No | `-` | Custom SQL query for reading |

**Examples**:

**PostgreSQL data generation**:
```yaml
dataSources:
  - name: my_postgres
    connection:
      type: postgres
      options:
        url: "jdbc:postgresql://localhost:5432/mydb"
        user: "postgres"
        password: "${POSTGRES_PASSWORD}"
    steps:
      - name: customers
        options:
          dbtable: "public.customers"
        count:
          records: 1000
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `database`, `jdbc`, `relational`, `sql`

---

### MySQL Connector

**ID**: `connector.databases.mysql`
**Status**: Stable

Connect to MySQL databases for reading and writing data. Supports table-level configuration and JDBC options.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | JDBC connection URL |
| `user` | string | No | `-` | Database username |
| `password` | string | No | `-` | Database password |
| `driver` | string | No | `com.mysql.cj.jdbc.Driver` | JDBC driver class |
| `dbtable` | string | No | `-` | Target table |

**Examples**:

**MySQL connection**:
```yaml
connection:
  type: mysql
  options:
    url: "jdbc:mysql://localhost:3306/mydb"
    user: "root"
    password: "${MYSQL_PASSWORD}"
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `database`, `jdbc`, `relational`, `sql`

---

### Cassandra Connector

**ID**: `connector.databases.cassandra`
**Status**: Stable

Connect to Apache Cassandra for reading and writing data. Supports keyspace/table configuration, primary key and clustering positions.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | Cassandra contact point URL |
| `user` | string | No | `-` | Cassandra username |
| `password` | string | No | `-` | Cassandra password |
| `keyspace` | string | Yes | `-` | Cassandra keyspace |
| `table` | string | Yes | `-` | Cassandra table |

**Examples**:

**Cassandra connection**:
```yaml
connection:
  type: cassandra
  options:
    url: "localhost:9042"
    user: "cassandra"
    password: "cassandra"
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `database`, `nosql`, `wide-column`

---

### BigQuery Connector

**ID**: `connector.databases.bigquery`
**Status**: Stable

Connect to Google BigQuery for reading and writing data. Supports direct and indirect write methods.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `table` | string | Yes | `-` | BigQuery table (project.dataset.table) |
| `credentialsFile` | string | No | `-` | Path to GCP credentials JSON |
| `writeMethod` | string | No | `indirect` | Write method Values: `direct`, `indirect` |
| `temporaryGcsBucket` | string | No | `-` | GCS bucket for indirect writes |
| `queryJobPriority` | string | No | `batch` | Query job priority |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `database`, `cloud`, `google`, `data-warehouse`

---

## File Formats

### CSV File Connector

**ID**: `connector.files.csv`
**Status**: Stable

Read and write CSV files. Supports headers, delimiters, and other CSV-specific options.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | File system path for CSV files YAML: `dataSources[].connection.options.path` |

**Examples**:

**CSV file output**:
```yaml
connection:
  type: csv
  options:
    path: "/tmp/data/csv-output"
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `csv`, `delimited`, `text`

---

### JSON File Connector

**ID**: `connector.files.json`
**Status**: Stable

Read and write JSON files. Supports nested structures, arrays, and unwrapping top-level arrays.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | File system path for JSON files |
| `unwrapTopLevelArray` | boolean | No | `false` | Output JSON as root-level array instead of object |

**Examples**:

**JSON file output**:
```yaml
connection:
  type: json
  options:
    path: "/tmp/data/json-output"
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `json`, `structured`

---

### Parquet File Connector

**ID**: `connector.files.parquet`
**Status**: Stable

Read and write Apache Parquet columnar files. Efficient for large datasets.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | File system path for Parquet files |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `parquet`, `columnar`, `binary`

---

### ORC File Connector

**ID**: `connector.files.orc`
**Status**: Stable

Read and write Apache ORC columnar files.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | File system path for ORC files |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `orc`, `columnar`, `binary`

---

### Delta Lake Connector

**ID**: `connector.files.delta`
**Status**: Stable

Read and write Delta Lake tables. Supports ACID transactions, time travel, and schema evolution.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | File system path for Delta tables |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `delta`, `lakehouse`, `acid`

---

### Apache Iceberg Connector

**ID**: `connector.files.iceberg`
**Status**: Stable

Read and write Apache Iceberg tables. Supports multiple catalog types (Hadoop, Hive, REST, Glue, JDBC, Nessie).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | Table path |
| `catalogType` | string | No | `hadoop` | Iceberg catalog type Values: `hadoop`, `hive`, `rest`, `glue`, `jdbc`, `nessie` |
| `catalogUri` | string | No | `-` | Catalog URI (for hive/rest/nessie) |
| `catalogDefaultNamespace` | string | No | `-` | Default namespace |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `iceberg`, `lakehouse`, `catalog`

---

### Apache Hudi Connector

**ID**: `connector.files.hudi`
**Status**: Stable

Read and write Apache Hudi tables.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | Table path |
| `hoodie.table.name` | string | Yes | `-` | Hudi table name |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `hudi`, `lakehouse`

---

### XML File Connector

**ID**: `connector.files.xml`
**Status**: Stable

Read and write XML files.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | `-` | File system path for XML files |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `file`, `xml`, `structured`

---

## Messaging Systems

### Apache Kafka Connector

**ID**: `connector.messaging.kafka`
**Status**: Stable

Connect to Apache Kafka for producing and consuming messages. Supports topics, partitions, headers, key/value serialization, and streaming patterns.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | Kafka bootstrap servers YAML: `dataSources[].connection.options.url` |
| `topic` | string | Yes | `-` | Kafka topic name |
| `schemaLocation` | string | No | `-` | Schema registry URL or file path |

**Examples**:

**Kafka streaming**:
```yaml
dataSources:
  - name: my_kafka
    connection:
      type: kafka
      options:
        url: "localhost:9092"
    steps:
      - name: orders_topic
        options:
          topic: "orders"
        count:
          duration: "1m"
          rate: 100
          rateUnit: "second"
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `messaging`, `kafka`, `streaming`, `event`

---

### Solace JMS Connector

**ID**: `connector.messaging.solace`
**Status**: Stable

Connect to Solace PubSub+ message broker via JMS. Supports queues and topics.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | Solace broker URL |
| `user` | string | No | `-` | Username |
| `password` | string | No | `-` | Password |
| `vpnName` | string | No | `default` | VPN name |
| `connectionFactory` | string | No | `/jms/cf/default` | JNDI connection factory |
| `initialContextFactory` | string | No | `-` | JNDI context factory |
| `destinationName` | string | Yes | `-` | Queue/topic destination |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `messaging`, `jms`, `solace`

---

### RabbitMQ Connector

**ID**: `connector.messaging.rabbitmq`
**Status**: Stable

Connect to RabbitMQ message broker via JMS. Supports queues.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | RabbitMQ URL |
| `user` | string | No | `guest` | Username |
| `password` | string | No | `guest` | Password |
| `virtualHost` | string | No | `/` | Virtual host |
| `destinationName` | string | Yes | `-` | Queue name |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `messaging`, `rabbitmq`, `jms`, `amqp`

---

## HTTP/REST

### HTTP/REST API Connector

**ID**: `connector.http.http`
**Status**: Stable

Send generated data to HTTP/REST APIs. Supports custom methods, headers, URL path parameters, query parameters, and request bodies.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | `-` | Base URL for HTTP requests |

**Examples**:

**HTTP API data generation**:
```yaml
dataSources:
  - name: my_api
    connection:
      type: http
      options:
        url: "http://localhost:8080"
    steps:
      - name: create_users
        fields:
          - name: httpUrl
            type: struct
            fields:
              - name: url
                static: "http://localhost:8080/api/users"
              - name: method
                static: "POST"
          - name: httpBody
            type: struct
            fields:
              - name: name
                options:
                  expression: "#{Name.fullName}"
```

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala` (supporting)

**Tags**: `http`, `rest`, `api`, `web`

---
