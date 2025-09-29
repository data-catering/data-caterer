# API Documentation

When running Data Caterer in UI mode, it exposes a REST API that allows you to interact with plans, connections, and data generation capabilities programmatically. This document outlines all available endpoints and their usage.

## Base URL

When running locally, the API is available at:

```
http://localhost:9898
```

## Authentication

Currently, the API does not require authentication. All endpoints are publicly accessible when the UI server is running.

## Content Type

All API requests that send data should use:

```
Content-Type: application/json
```

## API Endpoints

### Plan Execution

??? example "Execute Plan - `POST /run`"

    Execute a data generation plan.

    **Request Body:**

    ```json
    {
      "id": "my-plan-id",
      "plan": {
        "name": "my-plan",
        "description": "Plan description",
        "tasks": []
      },
      "tasks": [
        {
          "name": "task1",
          "dataSourceName": "my-datasource",
          "enabled": true
        }
      ],
      "validation": [],
      "configuration": {
        "flag": {},
        "folder": {},
        "metadata": {},
        "generation": {},
        "validation": {},
        "alert": {}
      }
    }
    ```

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Plan started",
      "executionId": "exec-12345"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/run \
          -H "Content-Type: application/json" \
          -d '{
            "id": "my-plan-id",
            "plan": {
              "name": "my-plan",
              "description": "Plan description"
            },
            "tasks": [
              {
                "name": "task1",
                "dataSourceName": "my-datasource",
                "enabled": true
              }
            ],
            "validation": [],
            "configuration": {
              "flag": {},
              "folder": {},
              "metadata": {},
              "generation": {},
              "validation": {},
              "alert": {}
            }
          }'
        ```

    === "wget"

        ```bash
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "id": "my-plan-id",
            "plan": {
              "name": "my-plan",
              "description": "Plan description"
            },
            "tasks": [
              {
                "name": "task1",
                "dataSourceName": "my-datasource",
                "enabled": true
              }
            ],
            "validation": [],
            "configuration": {
              "flag": {},
              "folder": {},
              "metadata": {},
              "generation": {},
              "validation": {},
              "alert": {}
            }
          }' \
          http://localhost:9898/run
        ```

??? example "Execute Plan (Delete Data Mode) - `POST /run/delete-data`"

    Execute a plan in delete data mode to clean up generated data.

    **Request Body:** Same as execute plan

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Plan delete data started",
      "executionId": "delete-exec-12345"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/run/delete-data \
          -H "Content-Type: application/json" \
          -d '{
            "id": "my-plan-id",
            "plan": {
              "name": "my-plan",
              "description": "Plan description"
            },
            "tasks": [
              {
                "name": "task1",
                "dataSourceName": "my-datasource",
                "enabled": true
              }
            ]
          }'
        ```

    === "wget"

        ```bash
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "id": "my-plan-id",
            "plan": {
              "name": "my-plan",
              "description": "Plan description"
            },
            "tasks": [
              {
                "name": "task1",
                "dataSourceName": "my-datasource",
                "enabled": true
              }
            ]
          }' \
          http://localhost:9898/run/delete-data
        ```

??? example "Get Plan Execution History - `GET /run/history`"

    Retrieve the history of all plan executions.

    **Response:** `200 OK`

    ```json
    [
      {
        "id": "execution-id",
        "status": "success",
        "failedReason": null,
        "runBy": "admin",
        "updatedBy": "admin",
        "createdTs": "2023-01-01T10:00:00Z",
        "updatedTs": "2023-01-01T10:05:00Z",
        "generationSummary": [],
        "validationSummary": [],
        "reportLink": "/report/execution-id/index.html",
        "timeTaken": "5m"
      }
    ]
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X GET http://localhost:9898/run/history
        ```

    === "wget"

        ```bash
        wget -O - http://localhost:9898/run/history
        ```

??? example "Get Plan Execution Status - `GET /run/status/{id}`"

    Get the current status of a specific plan execution.

    **Path Parameters:**

    - `id` - The execution ID (alphanumeric, hyphens, and underscores allowed)

    **Response:** `200 OK`

    ```json
    {
      "id": "execution-id",
      "status": "running",
      "failedReason": null,
      "runBy": "admin",
      "timeTaken": "2m"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X GET http://localhost:9898/run/status/execution-123
        ```

    === "wget"

        ```bash
        wget -O - http://localhost:9898/run/status/execution-123
        ```

### Plan Management

??? info "Save Plan - `POST /plan`"

    Save a plan configuration for later use.

    **Request Body:** Same as execute plan request

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Plan saved",
      "planId": "my-saved-plan"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/plan \
          -H "Content-Type: application/json" \
          -d '{
            "id": "my-saved-plan",
            "plan": {
              "name": "My Saved Plan",
              "description": "A plan for later use"
            },
            "tasks": [
              {
                "name": "task1",
                "dataSourceName": "my-datasource",
                "enabled": true
              }
            ]
          }'
        ```

    === "wget"

        ```bash
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "id": "my-saved-plan",
            "plan": {
              "name": "My Saved Plan",
              "description": "A plan for later use"
            },
            "tasks": [
              {
                "name": "task1",
                "dataSourceName": "my-datasource",
                "enabled": true
              }
            ]
          }' \
          http://localhost:9898/plan
        ```

??? info "Get Plan - `GET /plan/{planName}`"

    Retrieve a specific saved plan.

    **Path Parameters:**

    - `planName` - The name of the plan (alphanumeric, hyphens, and underscores allowed)

    **Response:** `200 OK`

    ```json
    {
      "id": "my-plan-id",
      "plan": {
        "name": "my-plan",
        "description": "Plan description"
      },
      "tasks": [],
      "validation": [],
      "configuration": {}
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X GET http://localhost:9898/plan/my-plan
        ```

    === "wget"

        ```bash
        wget -O - http://localhost:9898/plan/my-plan
        ```

??? info "Delete Plan - `DELETE /plan/{planName}`"

    Delete a saved plan.

    **Path Parameters:**

    - `planName` - The name of the plan to delete

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Plan removed",
      "planName": "my-plan"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X DELETE http://localhost:9898/plan/my-plan
        ```

    === "wget"

        ```bash
        wget --method=DELETE http://localhost:9898/plan/my-plan
        ```

??? info "List Plans - `GET /plans`"

    Get a list of all saved plans.

    **Response:** `200 OK`

    ```json
    [
      {
        "id": "plan1",
        "plan": {
          "name": "plan1",
          "description": "First plan"
        }
      },
      {
        "id": "plan2",
        "plan": {
          "name": "plan2",
          "description": "Second plan"
        }
      }
    ]
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X GET http://localhost:9898/plans
        ```

    === "wget"

        ```bash
        wget -O - http://localhost:9898/plans
        ```

### Connection Management

??? note "Save Connections - `POST /connection`"

    Save one or more connection configurations.

    **Request Body:**

    ```json
    {
      "connections": [
        {
          "name": "my-postgres",
          "type": "postgres",
          "groupType": "database",
          "options": {
            "host": "localhost",
            "port": "5432",
            "database": "mydb",
            "user": "username",
            "password": "password"
          }
        }
      ]
    }
    ```

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Connection saved",
      "connectionName": "my-postgres"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/connection \
          -H "Content-Type: application/json" \
          -d '{
            "connections": [
              {
                "name": "my-postgres",
                "type": "postgres",
                "groupType": "database",
                "options": {
                  "host": "localhost",
                  "port": "5432",
                  "database": "mydb",
                  "user": "username",
                  "password": "password"
                }
              }
            ]
          }'
        ```

    === "wget"

        ```bash
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "connections": [
              {
                "name": "my-postgres",
                "type": "postgres",
                "groupType": "database",
                "options": {
                  "host": "localhost",
                  "port": "5432",
                  "database": "mydb",
                  "user": "username",
                  "password": "password"
                }
              }
            ]
          }' \
          http://localhost:9898/connection
        ```

??? note "Get Connection - `GET /connection/{connectionName}`"

    Retrieve a specific connection configuration.

    **Path Parameters:**

    - `connectionName` - The name of the connection (alphanumeric, hyphens, and underscores allowed)

    **Response:** `200 OK`

    ```json
    {
      "name": "my-postgres",
      "type": "postgres",
      "groupType": "database",
      "options": {
        "host": "localhost",
        "port": "5432",
        "database": "mydb",
        "user": "username",
        "password": "***"
      }
    }
    ```

    **Note:** Sensitive fields like passwords and tokens are masked in responses.

    **Example:**

    === "curl"

        ```bash
        curl -X GET http://localhost:9898/connection/my-postgres
        ```

    === "wget"

        ```bash
        wget -O - http://localhost:9898/connection/my-postgres
        ```

??? note "Delete Connection - `DELETE /connection/{connectionName}`"

    Delete a connection configuration.

    **Path Parameters:**

    - `connectionName` - The name of the connection to delete

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Connection removed",
      "connectionName": "my-postgres"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X DELETE http://localhost:9898/connection/my-postgres
        ```

    === "wget"

        ```bash
        wget --method=DELETE http://localhost:9898/connection/my-postgres
        ```

??? note "List Connections - `GET /connections`"

    Get a list of all connections, optionally filtered by group type.

    **Query Parameters:**

    - `groupType` (optional) - Filter connections by group type (e.g., "database", "file", "messaging")

    **Response:** `200 OK`

    ```json
    {
      "connections": [
        {
          "name": "my-postgres",
          "type": "postgres",
          "groupType": "database",
          "options": {
            "host": "localhost",
            "port": "5432"
          }
        }
      ]
    }
    ```

    **Example:**

    === "curl"

        ```bash
        # Get all connections
        curl -X GET http://localhost:9898/connections
        
        # Filter by group type
        curl -X GET "http://localhost:9898/connections?groupType=database"
        ```

    === "wget"

        ```bash
        # Get all connections
        wget -O - http://localhost:9898/connections
        
        # Filter by group type
        wget -O - "http://localhost:9898/connections?groupType=database"
        ```

### Sample Data Generation

??? abstract "Generate Sample from Task File - `POST /sample/task-file`"

    Generate sample data based on a task file configuration.

    **Request Body:**

    ```json
    {
      "taskYamlPath": "/path/to/task.yaml",
      "stepName": "optional-step-name",
      "sampleSize": 10,
      "fastMode": true
    }
    ```

    **Response:** `200 OK`

    ```json
    {
      "success": true,
      "executionId": "sample-exec-123",
      "schema": {
        "fields": [
          {
            "name": "id",
            "type": "integer",
            "nullable": false
          },
          {
            "name": "name",
            "type": "string",
            "nullable": true
          }
        ]
      },
      "sampleData": [
        {
          "id": 1,
          "name": "John Doe"
        },
        {
          "id": 2,
          "name": "Jane Smith"
        }
      ],
      "metadata": {
        "sampleSize": 10,
        "actualRecords": 2,
        "generatedInMs": 150,
        "fastModeEnabled": true
      }
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/sample/task-file \
          -H "Content-Type: application/json" \
          -d '{
            "taskYamlPath": "/path/to/task.yaml",
            "stepName": "optional-step-name",
            "sampleSize": 10,
            "fastMode": true
          }'
        ```

    === "wget"

        ```bash
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "taskYamlPath": "/path/to/task.yaml",
            "stepName": "optional-step-name",
            "sampleSize": 10,
            "fastMode": true
          }' \
          http://localhost:9898/sample/task-file
        ```

??? abstract "Generate Sample from Task YAML Content - `POST /sample/task-yaml`"

    Generate sample data based on a task YAML content passed directly in the request body. This endpoint supports both JSON and raw YAML content types using custom unmarshallers.

    **Content Type Options:**

    1. **Raw YAML (recommended)**: Send YAML content directly with query parameters
       - **Content-Type**: `text/plain` or `application/yaml`
       - **Request Body**: Raw YAML task content
       - **Query Parameters**:
         - `stepName` (optional) - The name of the specific step to use for sample generation
         - `sampleSize` (optional) - Number of sample records to generate (default: 10)
         - `fastMode` (optional) - Enable fast generation mode (default: true)

    2. **JSON format** (backward compatibility): Send JSON with YAML content as string property
       - **Content-Type**: `application/json`
       - **Request Body**: JSON object with `taskYamlContent` field

    **Response:** `200 OK` - Same format as other sample endpoints

    **Error Handling:**
    The endpoint returns specific error codes for different failure scenarios:
    - `INVALID_YAML` - When YAML content cannot be parsed or contains invalid structure
    - `YAML_PARSE_ERROR` - When there are syntax errors in the YAML content  
    - `INVALID_REQUEST` - When required parameters are missing or invalid
    - `STEP_NOT_FOUND` - When the specified stepName doesn't exist in the YAML

    **Examples:**

    === "Raw YAML with curl"

        ```bash
        # Using a task YAML file with query parameters
        curl -X POST "http://localhost:9898/sample/task-yaml?stepName=file_account&sampleSize=5&fastMode=true" \
           -H "Content-Type: text/plain" \
           --data-binary '@app/src/test/resources/sample/task/file/simple-json-task.yaml'

        # Using inline YAML content with query parameters
        curl -X POST "http://localhost:9898/sample/task-yaml?stepName=test_step&sampleSize=5&fastMode=true" \
          -H "Content-Type: text/plain" \
          --data-binary @- <<'EOF'
        name: test_task
        steps:
        - name: test_step
          type: file
          fields:
            - name: id
              type: long
              options:
                min: 1
                max: 1000
            - name: name
              type: string
              options:
                expression: "#{Name.fullName}"
        EOF

        # Using application/yaml content type
        curl -X POST "http://localhost:9898/sample/task-yaml?stepName=test_step&sampleSize=3" \
          -H "Content-Type: application/yaml" \
          --data-binary @- <<'EOF'
        name: customer_task
        steps:
        - name: test_step
          type: file
          fields:
            - name: customer_id
              type: string
              options:
                regex: "CUST[0-9]{6}"
            - name: email
              type: string
              options:
                expression: "#{Internet.emailAddress}"
        EOF
        ```

    === "JSON with curl"

        ```bash
        # JSON format with parameters in request body
        curl -X POST http://localhost:9898/sample/task-yaml \
          -H "Content-Type: application/json" \
          -d '{
            "taskYamlContent": "name: test_task\nsteps:\n  - name: test_step\n    type: file\n    fields:\n      - name: id\n        type: long\n        options:\n          min: 1\n          max: 1000",
            "stepName": "test_step",
            "sampleSize": 5,
            "fastMode": true
          }'

        # JSON format with query parameters overriding request body
        curl -X POST "http://localhost:9898/sample/task-yaml?sampleSize=3&fastMode=false" \
          -H "Content-Type: application/json" \
          -d '{
            "taskYamlContent": "name: test_task\nsteps:\n  - name: test_step\n    type: file\n    fields:\n      - name: id\n        type: long\n        options:\n          min: 1\n          max: 1000",
            "stepName": "test_step",
            "sampleSize": 10,
            "fastMode": true
          }'
        ```

    === "Raw YAML with wget"

        ```bash
        # Using application/yaml content type
        wget --method=POST \
          --header="Content-Type: application/yaml" \
          --body-data='name: customer_task
        steps:
          - name: generate_customers
            type: file
            fields:
              - name: customer_id
                type: string
                options:
                  regex: "CUST[0-9]{6}"
              - name: name
                type: string
                options:
                  expression: "#{Name.fullName}"' \
          "http://localhost:9898/sample/task-yaml?stepName=generate_customers&sampleSize=8"
        ```

??? abstract "Generate Sample from Schema - `POST /sample/schema`"

    Generate sample data based on field definitions. This endpoint allows you to define fields directly without needing a complete step configuration.

    **Request Body:**

    ```json
    {
      "fields": [
        {
          "name": "account_id",
          "type": "string",
          "options": {
            "regex": "ACC[0-9]{10}"
          },
          "nullable": false
        },
        {
          "name": "balance",
          "type": "double",
          "options": {
            "min": 100.0,
            "max": 10000.0
          },
          "nullable": false
        },
        {
          "name": "customer_name", 
          "type": "string",
          "options": {
            "expression": "#{Name.fullName}"
          },
          "nullable": true
        }
      ],
      "sampleSize": 5,
      "fastMode": true
    }
    ```

    **Response:** `200 OK` - Same format as task file sample response

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/sample/schema \
          -H "Content-Type: application/json" \
          -d '{
            "fields": [
              {
                "name": "account_id",
                "type": "string",
                "options": {
                  "regex": "ACC[0-9]{10}"
                },
                "nullable": false
              },
              {
                "name": "balance",
                "type": "double", 
                "options": {
                  "min": 100.0,
                  "max": 10000.0
                },
                "nullable": false
              }
            ],
            "sampleSize": 5,
            "fastMode": true
          }'
        ```

    === "wget"

        ```bash
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "fields": [
              {
                "name": "account_id",
                "type": "string",
                "options": {
                  "regex": "ACC[0-9]{10}"
                },
                "nullable": false
              },
              {
                "name": "balance",
                "type": "double",
                "options": {
                  "min": 100.0,
                  "max": 10000.0
                },
                "nullable": false
              }
            ],
            "sampleSize": 5,
            "fastMode": true
          }' \
          http://localhost:9898/sample/schema
        ```

### Reports

??? tip "Get Report Resource - `GET /report/{runId}/{resource}`"

    Retrieve report resources (HTML, JSON, SVG, CSS files) for a specific plan execution.

    **Path Parameters:**

    - `runId` - UUID of the plan execution
    - `resource` - Name of the resource file (must end with .html, .json, .svg, or .css and be less than 50 characters)

    **Response:** `200 OK` - Returns the requested file content

    **Example:**

    === "curl"

        ```bash
        # Get HTML report
        curl -X GET http://localhost:9898/report/12345678-1234-1234-1234-123456789012/index.html
        
        # Get JSON data
        curl -X GET http://localhost:9898/report/12345678-1234-1234-1234-123456789012/data.json
        
        # Get CSS file
        curl -X GET http://localhost:9898/report/12345678-1234-1234-1234-123456789012/styles.css
        ```

    === "wget"

        ```bash
        # Get HTML report
        wget -O report.html http://localhost:9898/report/12345678-1234-1234-1234-123456789012/index.html
        
        # Get JSON data
        wget -O data.json http://localhost:9898/report/12345678-1234-1234-1234-123456789012/data.json
        
        # Get CSS file
        wget -O styles.css http://localhost:9898/report/12345678-1234-1234-1234-123456789012/styles.css
        ```

### System

??? success "Shutdown Server - `POST /shutdown`"

    Gracefully shutdown the Data Caterer server.

    **Response:** `200 OK`

    ```json
    {
      "status": "success",
      "message": "Data Caterer shutdown completed"
    }
    ```

    **Note:** This will terminate the server process.

    **Example:**

    === "curl"

        ```bash
        curl -X POST http://localhost:9898/shutdown
        ```

    === "wget"

        ```bash
        wget --method=POST http://localhost:9898/shutdown
        ```

---

## Error Responses

All endpoints may return error responses in the following format:

**Response:** `500 Internal Server Error`

```json
{
  "status": "error",
  "message": "Failed to execute request",
  "uri": "/api/endpoint",
  "errorMessage": "Error details"
}
```

**Response:** `400 Bad Request`

```json
{
  "status": "error",
  "message": "Unable to fetch resource for request",
  "details": "Invalid request parameters"
}
```

---

## Request/Response Models

### PlanRunRequest

```json
{
  "id": "string",
  "plan": {
    "name": "string",
    "description": "string"
  },
  "tasks": [
    {
      "name": "string",
      "dataSourceName": "string",
      "enabled": true
    }
  ],
  "validation": [],
  "configuration": {
    "flag": {},
    "folder": {},
    "metadata": {},
    "generation": {},
    "validation": {},
    "alert": {}
  }
}
```

### Connection

```json
{
  "name": "string",
  "type": "string",
  "groupType": "string",
  "options": {
    "key": "value"
  }
}
```

### Sample Generation Response

```json
{
  "success": true,
  "executionId": "string",
  "schema": {
    "fields": [
      {
        "name": "string",
        "type": "string",
        "nullable": true,
        "fields": []
      }
    ]
  },
  "sampleData": [
    {
      "field1": "value1",
      "field2": "value2"
    }
  ],
  "metadata": {
    "sampleSize": 10,
    "actualRecords": 10,
    "generatedInMs": 100,
    "fastModeEnabled": true
  },
  "error": {
    "code": "string",
    "message": "string",
    "details": "string"
  }
}
```

---

## Quick Start

Each endpoint above includes copy-paste ready examples for both `curl` and `wget`. Choose your preferred tool using the tabs in each endpoint section.

**Common Usage Patterns:**

- **Plan Management**: Use `/run` to execute plans, `/plan` to save/retrieve configurations
- **Connection Setup**: Use `/connection` to manage data source connections  
- **Sample Generation**: Use `/sample/task-file` or `/sample/schema` for quick data previews
- **Monitoring**: Use `/run/history` and `/run/status/{id}` to track execution progress
- **Reports**: Use `/report/{runId}/{resource}` to access generated reports

This API provides comprehensive access to Data Caterer's functionality, allowing you to integrate data generation capabilities into your applications and workflows.
