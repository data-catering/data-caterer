# Custom Transformations

Apply custom logic to generated files after they are written. This "last mile" transformation enables converting to custom formats, applying business-specific transformations, or restructuring data.

## Overview

Transformations execute **after** Data Caterer generates and writes data to files. They operate in two modes:

- **Per-Record**: Transform each line/record individually
- **Whole-File**: Transform the entire file as a unit

## Per-Record Transformation

Transform each record/line in a file independently. Best for line-by-line modifications like formatting, prefixing, or simple data mapping.

=== "Java"

    ```java
    var task = csv("my_csv", "/path/to/csv")
        .fields(
            field().name("account_id"),
            field().name("name")
        )
        .count(count().records(100))
        .transformationPerRecord(
            "com.example.MyPerRecordTransformer",
            "transformRecord"
        )
        .transformationOptions(Map.of(
            "prefix", "BANK_",
            "suffix", "_VERIFIED"
        ));
    ```

=== "Scala"

    ```scala
    val task = csv("my_csv", "/path/to/csv")
      .fields(
        field.name("account_id"),
        field.name("name")
      )
      .count(count.records(100))
      .transformationPerRecord(
        "com.example.MyPerRecordTransformer",
        "transformRecord"
      )
      .transformationOptions(Map(
        "prefix" -> "BANK_",
        "suffix" -> "_VERIFIED"
      ))
    ```

=== "YAML"

    ```yaml
    name: "csv_transform_plan"

    dataSources:
      - name: "csv_accounts"
        connection:
          type: "csv"
          options:
            path: "/tmp/accounts.csv"
        steps:
          - name: "accounts"
            count:
              records: 100
            fields:
              - name: "account_id"
              - name: "name"
            transformation:
              className: "com.example.MyPerRecordTransformer"
              methodName: "transformRecord"
              mode: "per-record"
              options:
                prefix: "BANK_"
                suffix: "_VERIFIED"
    ```

### Implementing Per-Record Transformer

=== "Java"

    ```java
    package com.example;

    import java.util.Map;

    public class MyPerRecordTransformer {
        public String transformRecord(String record, Map<String, String> options) {
            String prefix = options.getOrDefault("prefix", "");
            String suffix = options.getOrDefault("suffix", "");
            return prefix + record + suffix;
        }
    }
    ```

=== "Scala"

    ```scala
    package com.example

    class MyPerRecordTransformer {
      def transformRecord(record: String, options: Map[String, String]): String = {
        val prefix = options.getOrElse("prefix", "")
        val suffix = options.getOrElse("suffix", "")
        s"$prefix$record$suffix"
      }
    }
    ```

## Whole-File Transformation

Transform the entire file as a single unit. Best for operations requiring full file context like wrapping JSON objects in arrays, adding headers/footers, or format conversions.

=== "Java"

    ```java
    var task = json("my_json", "/path/to/json")
        .fields(
            field().name("id"),
            field().name("data")
        )
        .count(count().records(200))
        .transformationWholeFile(
            "com.example.JsonArrayWrapperTransformer",
            "transformFile"
        )
        .transformationOptions(Map.of("minify", "true"));
    ```

=== "Scala"

    ```scala
    val task = json("my_json", "/path/to/json")
      .fields(
        field.name("id"),
        field.name("data")
      )
      .count(count.records(200))
      .transformationWholeFile(
        "com.example.JsonArrayWrapperTransformer",
        "transformFile"
      )
      .transformationOptions(Map("minify" -> "true"))
    ```

=== "YAML"

    ```yaml
    name: "json_transform_plan"

    dataSources:
      - name: "my_json"
        connection:
          type: "json"
          options:
            path: "/tmp/transactions.json"
        steps:
          - name: "transactions"
            count:
              records: 200
            fields:
              - name: "id"
              - name: "data"
            transformation:
              className: "com.example.JsonArrayWrapperTransformer"
              methodName: "transformFile"
              mode: "whole-file"
              options:
                minify: "true"
    ```

### Implementing Whole-File Transformer

=== "Java"

    ```java
    package com.example;

    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.util.Map;

    public class JsonArrayWrapperTransformer {
        public void transformFile(String inputPath, String outputPath,
                                 Map<String, String> options) throws Exception {
            String content = Files.readString(Path.of(inputPath));
            String[] lines = content.split("\\n");

            boolean minify = Boolean.parseBoolean(
                options.getOrDefault("minify", "false")
            );

            StringBuilder result = new StringBuilder("[");
            if (!minify) result.append("\n");

            for (int i = 0; i < lines.length; i++) {
                if (!minify) result.append("  ");
                result.append(lines[i]);
                if (i < lines.length - 1) result.append(",");
                if (!minify) result.append("\n");
            }

            if (!minify) result.append("\n");
            result.append("]");

            Files.writeString(Path.of(outputPath), result.toString());
        }
    }
    ```

=== "Scala"

    ```scala
    package com.example

    import java.nio.file.{Files, Paths}
    import scala.io.Source

    class JsonArrayWrapperTransformer {
      def transformFile(inputPath: String, outputPath: String,
                       options: Map[String, String]): Unit = {
        val content = Source.fromFile(inputPath).mkString
        val lines = content.split("\n")

        val minify = options.getOrElse("minify", "false").toBoolean
        val separator = if (minify) "" else "\n"
        val indent = if (minify) "" else "  "

        val wrapped = lines.zipWithIndex.map { case (line, idx) =>
          val comma = if (idx < lines.length - 1) "," else ""
          s"$indent$line$comma"
        }.mkString(separator)

        val result = if (minify) {
          s"[$wrapped]"
        } else {
          s"[$separator$wrapped$separator]"
        }

        Files.writeString(Paths.get(outputPath), result)
      }
    }
    ```

## Task-Level Transformation

Apply the same transformation to all steps in a task.

=== "Java"

    ```java
    var myTask = csv("my_csv", "/path")
        .fields(...)
        .transformation("com.example.MyTransformer")
        .transformationOptions(Map.of("format", "custom"));
    ```

=== "Scala"

    ```scala
    val myTask = csv("my_csv", "/path")
      .fields(...)
      .transformation("com.example.MyTransformer")
      .transformationOptions(Map("format" -> "custom"))
    ```

=== "YAML"

    ```yaml
    name: "my_transform_plan"

    dataSources:
      - name: "my_csv"
        connection:
          type: "csv"
          options:
            path: "/path"
        steps:
          - name: "step1"
            transformation:
              className: "com.example.MyTransformer"
              mode: "whole-file"
              options:
                format: "custom"
            # ...
    ```

## Custom Output Path

Save transformed files to a different location. Optionally delete the original.

=== "Java"

    ```java
    var task = csv("my_csv", "/path/to/original")
        .fields(...)
        .transformationPerRecord("com.example.Transformer")
        .transformationOutput(
            "/path/to/transformed/output.csv",
            true  // Delete original
        );
    ```

=== "Scala"

    ```scala
    val task = csv("my_csv", "/path/to/original")
      .fields(...)
      .transformationPerRecord("com.example.Transformer")
      .transformationOutput(
        "/path/to/transformed/output.csv",
        true  // Delete original
      )
    ```

=== "YAML"

    ```yaml
    transformation:
      className: "com.example.Transformer"
      methodName: "transformRecord"
      mode: "per-record"
      outputPath: "/path/to/transformed/output.csv"
      deleteOriginal: true
    ```

## Enable/Disable Transformation

Control transformation execution dynamically.

=== "Java"

    ```java
    var task = json("my_json", "/path")
        .fields(...)
        .transformationWholeFile("com.example.Transformer")
        .enableTransformation(
            Boolean.parseBoolean(
                System.getenv().getOrDefault("ENABLE_TRANSFORM", "false")
            )
        );
    ```

=== "Scala"

    ```scala
    val task = json("my_json", "/path")
      .fields(...)
      .transformationWholeFile("com.example.Transformer")
      .enableTransformation(
        sys.env.getOrElse("ENABLE_TRANSFORM", "false").toBoolean
      )
    ```

=== "YAML"

    ```yaml
    transformation:
      className: "com.example.Transformer"
      enabled: false  # Can be overridden at runtime
    ```

## Configuration Reference

| Property         | Type    | Default                                                            | Description                               |
| ---------------- | ------- | ------------------------------------------------------------------ | ----------------------------------------- |
| `className`      | String  | Required                                                           | Fully qualified class name of transformer |
| `methodName`     | String  | `"transformRecord"` (per-record)<br>`"transformFile"` (whole-file) | Method to invoke                          |
| `mode`           | String  | `"whole-file"`                                                     | `"per-record"` or `"whole-file"`          |
| `outputPath`     | String  | Original path                                                      | Custom output path (optional)             |
| `deleteOriginal` | Boolean | `false`                                                            | Delete original after transformation      |
| `options`        | Map     | `{}`                                                               | Custom options passed to transformer      |
| `enabled`        | Boolean | `true`                                                             | Enable/disable transformation             |

## Use Cases

### Convert JSON Lines to Array

```java
// Wraps individual JSON objects into a JSON array
.transformationWholeFile("com.example.JsonArrayWrapperTransformer")
```

### Add CSV Header

```java
// Prepend custom header to CSV file
.transformationWholeFile("com.example.CsvHeaderTransformer")
  .transformationOptions(Map.of("header", "ID,Name,Amount"))
```

### Format for Legacy Systems

```java
// Convert to fixed-width format
.transformationPerRecord("com.example.FixedWidthFormatter")
  .transformationOptions(Map.of(
    "id_width", "10",
    "name_width", "30",
    "amount_width", "15"
  ))
```

### Encrypt Sensitive Fields

```java
// Apply encryption line-by-line
.transformationPerRecord("com.example.FieldEncryptor")
  .transformationOptions(Map.of(
    "fields", "ssn,credit_card",
    "algorithm", "AES"
  ))
```

## Deployment with Custom Transformers

To use custom transformers in production, you need to make them available in the Data Caterer classpath.

### Docker Deployment

#### Option 1: Volume Mount Custom JARs

Mount your transformer JAR(s) into the `/opt/app/custom` directory, which is automatically included in the classpath.

```bash
# Build your transformer JAR
./gradlew clean build

# Run Data Caterer with custom transformer
docker run -d \
  -v /path/to/your-transformers.jar:/opt/app/custom/transformers.jar \
  -v /path/to/config:/opt/DataCaterer/plan \
  datacatering/data-caterer:0.17.0
```

#### Option 2: Build Custom Docker Image

Create a multi-stage Dockerfile that includes your transformers:

```dockerfile
# Stage 1: Build your transformer JAR
FROM gradle:8.11.1-jdk17 AS builder
WORKDIR /build

COPY gradle ./gradle
COPY gradlew gradlew.bat build.gradle.kts settings.gradle.kts ./
COPY buildSrc ./buildSrc

# Download dependencies (cached layer)
RUN ./gradlew dependencies --no-daemon || true

# Copy source and build
COPY src ./src
RUN ./gradlew clean build --no-daemon

# Stage 2: Runtime with Data Caterer
ARG DATA_CATERER_VERSION=0.17.0
FROM datacatering/data-caterer:${DATA_CATERER_VERSION}

# Copy transformer JAR to custom directory
COPY --from=builder --chown=app:app /build/build/libs/transformers.jar /opt/app/custom/transformers.jar

# Optionally copy your plan files
COPY --chown=app:app plan /opt/DataCaterer/plan
```

Build and run:

```bash
docker build -t my-data-caterer:latest .
docker run -d my-data-caterer:latest
```

#### Option 3: Extend Base Image

For simple cases, extend the base image directly:

```dockerfile
ARG DATA_CATERER_VERSION=0.17.0
FROM datacatering/data-caterer:${DATA_CATERER_VERSION}

# Copy pre-built transformer JAR
COPY --chown=app:app target/my-transformers.jar /opt/app/custom/

# Copy configuration
COPY --chown=app:app config/plan.yaml /opt/DataCaterer/plan/
```

### Kubernetes/Helm Deployment

#### Using ConfigMaps and PersistentVolumes

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-caterer-plan
data:
  plan.yaml: |
    name: "my_plan"
    # ... your plan configuration
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: custom-transformers
spec:
  accessModes:
    - ReadOnlyMany
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-caterer
spec:
  template:
    spec:
      containers:
      - name: data-caterer
        image: datacatering/data-caterer:0.17.0
        volumeMounts:
        - name: plan
          mountPath: /opt/DataCaterer/plan
        - name: custom-jars
          mountPath: /opt/app/custom
      volumes:
      - name: plan
        configMap:
          name: data-caterer-plan
      - name: custom-jars
        persistentVolumeClaim:
          claimName: custom-transformers
```

#### Using InitContainers

Download transformer JARs at runtime:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-caterer
spec:
  template:
    spec:
      initContainers:
      - name: fetch-transformers
        image: curlimages/curl:latest
        command:
        - sh
        - -c
        - |
          curl -L -o /custom/transformers.jar \
            https://your-repo.com/transformers.jar
        volumeMounts:
        - name: custom-jars
          mountPath: /custom
      containers:
      - name: data-caterer
        image: datacatering/data-caterer:0.17.0
        volumeMounts:
        - name: custom-jars
          mountPath: /opt/app/custom
      volumes:
      - name: custom-jars
        emptyDir: {}
```

### Local/Application Deployment

When running Data Caterer as a standalone application or via Spark submit:

#### Add to Classpath

```bash
# Using spark-submit
spark-submit \
  --class io.github.datacatering.datacaterer.App \
  --master local[*] \
  --jars /path/to/your-transformers.jar \
  data-caterer.jar

# Using java directly
java -cp "data-caterer.jar:/path/to/your-transformers.jar" \
  io.github.datacatering.datacaterer.App
```

#### Gradle Configuration

Add transformer dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.data-catering:data-caterer-api:0.17.0")
    implementation(files("libs/your-transformers.jar"))
}

tasks.register<JavaExec>("runWithTransformers") {
    mainClass.set("io.github.datacatering.datacaterer.App")
    classpath = sourceSets["main"].runtimeClasspath
}
```

### Project Structure for Transformers

Recommended project structure:

```
my-data-project/
├── build.gradle.kts
├── src/
│   ├── main/
│   │   ├── java/com/mycompany/
│   │   │   └── transformers/
│   │   │       ├── CustomPerRecordTransformer.java
│   │   │       └── CustomWholeFileTransformer.java
│   │   └── scala/com/mycompany/
│   │       └── plans/
│   │           └── MyDataPlan.scala
│   └── test/
│       └── java/com/mycompany/
│           └── transformers/
│               └── TransformerTest.java
├── plan/
│   └── my-plan.yaml
└── Dockerfile
```

#### Example build.gradle.kts

```kotlin
plugins {
    java
    scala
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.mycompany"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.data-catering:data-caterer-api:0.17.0")
    implementation("org.scala-lang:scala-library:2.12.18")
    testImplementation("junit:junit:4.13.2")
}

tasks.shadowJar {
    archiveBaseName.set("my-transformers")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
}
```

## Best Practices

1. **Use per-record for line-by-line operations** - More memory efficient
2. **Use whole-file for structural changes** - When context across records is needed
3. **Keep transformers stateless** - For reliability and reusability
4. **Handle errors gracefully** - Log issues without failing the entire transformation
5. **Make transformers configurable** - Use options for flexibility
6. **Test transformers independently** - Unit test transformation logic separately
7. **Package transformers separately** - Keep transformers in their own JAR for reusability
8. **Use meaningful class names** - Makes configuration and debugging easier
9. **Document transformer options** - Clearly specify required and optional parameters
10. **Version your transformers** - Especially when deploying to production

## API/Sample Endpoint Support

Transformations are automatically applied when using Data Caterer's REST API sample endpoints. This enables you to preview transformed data through the API.

### How It Works

When you configure a transformation on a step, it will be applied when generating samples via:

- `GET /sample/plans/{planName}` - Entire plan with all steps
- `GET /sample/plans/{planName}/tasks/{taskName}` - Specific task
- `GET /sample/plans/{planName}/tasks/{taskName}/steps/{stepName}` - Specific step
- `GET /sample/tasks/{taskName}` - By task name
- `GET /sample/steps/{stepName}` - By step name

### Example Usage

**1. Configure transformation in your task:**

=== "YAML"

    ```yaml
    name: "sample_transform_plan"

    dataSources:
      - name: "sample_json"
        connection:
          type: "json"
          options:
            path: "/tmp/transactions.json"
        steps:
          - name: "transactions"
            count:
              records: 10
            fields:
              - name: "id"
              - name: "amount"
            transformation:
              className: "com.example.JsonArrayWrapperTransformer"
              methodName: "transformFile"
              mode: "whole-file"
              enabled: true
    ```

**2. Generate sample with transformation applied:**

=== "curl"

    ```bash
    # Get transformed sample data
    curl "http://localhost:9898/sample/steps/transactions?sampleSize=5"
    
    # Response will have transformation applied
    # Instead of: {"id":"1","amount":100}
    #             {"id":"2","amount":200}
    # 
    # You get:    [{"id":"1","amount":100},{"id":"2","amount":200}]
    ```

=== "Browser"

    ```
    http://localhost:9898/sample/steps/transactions?sampleSize=5
    ```

### Benefits

✅ **Preview transformations** - See transformed output before full generation  
✅ **Test transformers** - Validate transformer logic with sample data  
✅ **API integration** - External systems can request transformed samples  
✅ **Development workflow** - Quick feedback during transformer development  

### Notes

- Transformations are applied to the generated sample data before returning it
- Per-record transformations process each line individually
- Whole-file transformations process the entire sample as a unit
- Transformation options (from configuration) are passed to the transformer
- Same transformer classes work for both full generation and sampling

See the [API Documentation](../api.md#sample-generation) for more details on sample endpoints.

## Example Transformers

See the example transformers in the repository:

- [UpperCasePerRecordTransformer.scala](https://github.com/data-catering/data-caterer/tree/main/example/src/main/scala/io/github/datacatering/transformer/UpperCasePerRecordTransformer.scala)
- [JsonArrayWrapperTransformer.scala](https://github.com/data-catering/data-caterer/tree/main/example/src/main/scala/io/github/datacatering/transformer/JsonArrayWrapperTransformer.scala)
- [TransformationExamplePlan.scala](https://github.com/data-catering/data-caterer/tree/main/example/src/main/scala/io/github/datacatering/plan/TransformationExamplePlan.scala)
