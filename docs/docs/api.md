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

        # Alternative endpoint (same functionality)
        curl -X GET http://localhost:9898/run/executions/execution-123
        ```

    === "wget"

        ```bash
        wget -O - http://localhost:9898/run/status/execution-123

        # Alternative endpoint (same functionality)
        wget -O - http://localhost:9898/run/executions/execution-123
        ```

    **Note:** Both `/run/status/{id}` and `/run/executions/{id}` provide the same functionality.

??? example "Execute Plan by Name - `POST /run/plans/{planName}`"

    Execute a saved plan by name with enhanced control options. This is the recommended way to execute YAML-defined plans.

    **Path Parameters:**

    - `planName` - The name of the plan to execute (alphanumeric, hyphens, and underscores allowed)

    **Query Parameters:**

    - `source` (optional, default: "auto") - Source type for plan loading
    - `mode` (optional, default: "generate") - Execution mode: "generate" or "delete"
    - `fileName` (optional) - Specific file name for plan loading

    **Response:** `200 OK`

    ```
    Plan 'my-plan' started in generate mode
    ```

    **Example:**

    === "curl"

        ```bash
        # Execute plan with default parameters
        curl -X POST http://localhost:9898/run/plans/my-customer-plan

        # Execute in delete mode
        curl -X POST "http://localhost:9898/run/plans/my-customer-plan?mode=delete"

        # Execute with specific source and file
        curl -X POST "http://localhost:9898/run/plans/my-plan?source=yaml&fileName=custom-plan.yaml"
        ```

    === "wget"

        ```bash
        # Execute plan with default parameters
        wget --method=POST http://localhost:9898/run/plans/my-customer-plan

        # Execute in delete mode
        wget --method=POST "http://localhost:9898/run/plans/my-customer-plan?mode=delete"
        ```

??? example "Execute Plan Task - `POST /run/plans/{planName}/tasks/{taskName}`"

    Execute a specific task within a saved plan. This allows selective execution of individual tasks without running the entire plan.

    **Path Parameters:**

    - `planName` - The name of the plan
    - `taskName` - The name of the task to execute

    **Query Parameters:**

    - `source` (optional, default: "auto") - Source type for plan loading
    - `mode` (optional, default: "generate") - Execution mode: "generate" or "delete"
    - `fileName` (optional) - Specific file name for plan loading

    **Response:** `200 OK`

    ```
    Task 'json_account_file' in plan 'account-plan' started in generate mode
    ```

    **Example:**

    === "curl"

        ```bash
        # Execute specific task
        curl -X POST http://localhost:9898/run/plans/account-plan/tasks/json_account_file

        # Execute task in delete mode
        curl -X POST "http://localhost:9898/run/plans/account-plan/tasks/json_account_file?mode=delete"
        ```

    === "wget"

        ```bash
        wget --method=POST http://localhost:9898/run/plans/account-plan/tasks/json_account_file
        ```

??? example "Execute Plan Step - `POST /run/plans/{planName}/tasks/{taskName}/steps/{stepName}`"

    Execute a specific step within a task in a saved plan. This provides the most granular control over plan execution.

    **Path Parameters:**

    - `planName` - The name of the plan
    - `taskName` - The name of the task
    - `stepName` - The name of the step to execute

    **Query Parameters:**

    - `source` (optional, default: "auto") - Source type for plan loading
    - `mode` (optional, default: "generate") - Execution mode: "generate" or "delete"
    - `fileName` (optional) - Specific file name for plan loading

    **Response:** `200 OK`

    ```
    Step 'file_account' in task 'json_account_file' of plan 'account-plan' started in generate mode
    ```

    **Example:**

    === "curl"

        ```bash
        # Execute specific step
        curl -X POST http://localhost:9898/run/plans/account-plan/tasks/json_account_file/steps/file_account

        # Execute step in delete mode
        curl -X POST "http://localhost:9898/run/plans/account-plan/tasks/json_account_file/steps/file_account?mode=delete"
        ```

    === "wget"

        ```bash
        wget --method=POST http://localhost:9898/run/plans/account-plan/tasks/json_account_file/steps/file_account
        ```

### Plan Management

!!! tip "YAML Plans and Connection Configuration"

    Data Caterer supports executing YAML-defined plans and tasks through the UI/API. When you execute a YAML plan via the API or UI:

    **Connection Configuration Integration:**

    - Connection details defined in `application.conf` are automatically loaded and merged with task configurations
    - The connection format (e.g., `json`, `postgres`, `kafka`) is determined by the top-level key in your configuration:
      ```
      json {
        my_json_connection {
          path = "/data/output"
          saveMode = "overwrite"
        }
      }
      ```
    - This configuration is automatically merged into task step options, so you don't need to duplicate connection details in your YAML task files

    **Example Workflow:**

    1. Define connections in `application.conf`:
       ```
       postgres {
         customer_db {
           url = "jdbc:postgresql://localhost:5432/customer"
           user = "admin"
           password = ${?DB_PASSWORD}
           driver = "org.postgresql.Driver"
         }
       }
       ```

    2. Create YAML task file referencing the connection:
       ```yaml
       name: "customer_task"
       steps:
         - name: "customers_table"
           type: "table"
           options:
             dbtable: "customers"
           fields:
             - name: "customer_id"
               type: "string"
       ```

    3. Create YAML plan referencing the task:
       ```yaml
       name: "customer_data_plan"
       description: "Generate customer data"
       tasks:
         - name: "customer_task"
           dataSourceName: "customer_db"
           enabled: true
       ```

    4. Execute via API - connection details are automatically merged from `application.conf`

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

!!! info "Sample Generation Parameters"

    All sample endpoints support the following query parameters:

    **fastMode** (boolean, default: true)

    Controls the data generation strategy:

    **Fast Mode Enabled (`fastMode=true`, default):**

    - Uses SQL-only generators for maximum performance
    - Response times: 80-141ms for most requests (after warm-up)
    - Ideal for quick API usage and interactive applications
    - Best for simple to moderately complex data generation
    - Recommended for production use

    **Fast Mode Disabled (`fastMode=false`):**

    - Uses advanced generators with Faker library support, regex, etc.
    - Response times: 150-210ms for most requests
    - Supports more complex data generation patterns
    - Required for some advanced expression fields
    - Use when you need specific Faker expressions

    **Performance Comparison:**

    - Fast mode is **45-58% faster** for complex field generation
    - Fast mode shows more consistent performance across requests
    - Both modes scale efficiently (50 records ≈ 10 records in execution time)

    **When to use each mode:**

    - ✅ **Fast Mode ON**: Default choice for most use cases, APIs, and interactive tools
    - ⚠️ **Fast Mode OFF**: Only when you specifically need data to meet all options specified

    **enableRelationships** (boolean, default: false)

    Enables relationship-based data generation across multiple steps:

    - When enabled, generates data with referential integrity between related fields (e.g., foreign keys)
    - Useful for generating realistic datasets with parent-child relationships
    - Automatically detected from metadata or explicit field configurations
    - Response metadata includes `relationshipsEnabled` field indicating if relationships were used

??? abstract "Generate Sample from Saved Plan - `GET /sample/plans/{planName}`"

    Generate sample data from an entire saved plan configuration. This endpoint loads a plan by name and generates sample data for all enabled tasks and steps within it.

    **Path Parameters:**

    - `planName` - The name of the saved plan (alphanumeric, hyphens, and underscores allowed)

    **Query Parameters:**

    - `sampleSize` (optional, default: 10) - Number of sample records to generate per step
    - `fastMode` (optional, default: true) - Enable fast generation mode
    - `enableRelationships` (optional, default: false) - Enable relationship-based data generation

    **Response:** `200 OK` (MultiSchemaSampleResponse)

    Returns a JSON object with samples grouped by task/step name. Each task generates sample data for all its steps.

    ```json
    {
      "success": true,
      "executionId": "a8b3c4d5",
      "samples": {
        "customer-plan/customer_data": [
          {
            "customer_id": "CUST000123",
            "name": "John Smith",
            "email": "john.smith@example.com",
            "created_at": "2024-01-15T10:30:00Z"
          },
          {
            "customer_id": "CUST000124",
            "name": "Jane Doe",
            "email": "jane.doe@example.com",
            "created_at": "2024-01-15T11:45:00Z"
          }
        ],
        "customer-plan/account_data": [
          {
            "account_id": "ACC1234567890",
            "customer_id": "CUST000123",
            "balance": 5432.10,
            "account_type": "checking"
          },
          {
            "account_id": "ACC9876543210",
            "customer_id": "CUST000124",
            "balance": 12500.50,
            "account_type": "savings"
          }
        ]
      },
      "metadata": {
        "sampleSize": 2,
        "actualRecords": 4,
        "generatedInMs": 245,
        "fastModeEnabled": true
      }
    }
    ```

    **Error Response:** `400 Bad Request`

    ```json
    {
      "code": "GENERATION_ERROR",
      "message": "Plan not found: my-customer-plan (searched in JSON and YAML sources)",
      "details": "FileNotFoundException"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        # Generate sample from plan with default parameters
        curl -X GET http://localhost:9898/sample/plans/my-customer-plan

        # Generate with custom sample size and fast mode disabled
        curl -X GET "http://localhost:9898/sample/plans/my-customer-plan?sampleSize=20&fastMode=false"
        ```

    === "wget"

        ```bash
        # Generate sample from plan with default parameters
        wget -O - http://localhost:9898/sample/plans/my-customer-plan

        # Generate with custom sample size
        wget -O - "http://localhost:9898/sample/plans/my-customer-plan?sampleSize=20&fastMode=true"
        ```

??? abstract "Generate Sample from Task - `GET /sample/tasks/{taskName}`"

    Generate sample data from all steps within a specific task by its name. This endpoint searches for the task across all available task files and generates samples for all steps within that task.

    **Path Parameters:**

    - `taskName` - The name of the task (alphanumeric, hyphens, and underscores allowed)

    **Query Parameters:**

    - `sampleSize` (optional, default: 10) - Number of sample records to generate per step
    - `fastMode` (optional, default: true) - Enable fast generation mode
    - `enableRelationships` (optional, default: false) - Enable relationship-based data generation

    **Response:** `200 OK` (MultiSchemaSampleResponse)

    Returns a JSON object with samples grouped by step name. Each step within the task generates the specified number of sample records.

    ```json
    {
      "success": true,
      "executionId": "b5c9a2f1",
      "samples": {
        "customer_details_step": [
          {
            "customer_id": "CUST001234",
            "name": "John Smith",
            "email": "john.smith@example.com",
            "age": 34,
            "created_at": "2024-01-15T10:30:00Z"
          },
          {
            "customer_id": "CUST001235",
            "name": "Jane Doe", 
            "email": "jane.doe@example.com",
            "age": 28,
            "created_at": "2024-01-15T11:45:00Z"
          }
        ],
        "customer_addresses_step": [
          {
            "customer_id": "CUST001234",
            "address_id": "ADDR567890",
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip_code": "62701"
          },
          {
            "customer_id": "CUST001235", 
            "address_id": "ADDR567891",
            "street": "456 Oak Ave",
            "city": "Chicago",
            "state": "IL", 
            "zip_code": "60601"
          }
        ]
      },
      "metadata": {
        "sampleSize": 2,
        "actualRecords": 4,
        "generatedInMs": 180,
        "fastModeEnabled": true
      }
    }
    ```

    **Error Response:** `400 Bad Request`

    ```json
    {
      "code": "TASK_SAMPLE_ERROR",
      "message": "Task 'nonexistent_task' not found in task folder: /path/to/tasks. Available tasks: customer_task, account_task, transaction_task",
      "details": "IllegalArgumentException"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        # Generate sample from task with default parameters
        curl -X GET http://localhost:9898/sample/tasks/customer_task

        # Generate with custom sample size and fast mode disabled
        curl -X GET "http://localhost:9898/sample/tasks/customer_task?sampleSize=25&fastMode=false"

        # Generate samples with relationships enabled
        curl -X GET "http://localhost:9898/sample/tasks/customer_task?sampleSize=10&enableRelationships=true"
        ```

    === "wget"

        ```bash
        # Generate sample from task with default parameters
        wget -O - http://localhost:9898/sample/tasks/customer_task

        # Generate with custom sample size
        wget -O - "http://localhost:9898/sample/tasks/customer_task?sampleSize=25&fastMode=true"
        ```

??? abstract "Generate Sample from Step - `GET /sample/steps/{stepName}`"

    Generate sample data from a specific step by its name. This endpoint searches for the step across all available tasks and generates sample data using that step's configuration. Returns **raw data** in the step's configured format.

    **Path Parameters:**

    - `stepName` - The name of the step (alphanumeric, hyphens, and underscores allowed)

    **Query Parameters:**

    - `sampleSize` (optional, default: 10) - Number of sample records to generate
    - `fastMode` (optional, default: true) - Enable fast generation mode

    **Response:** `200 OK` - Returns **raw data** in the step's configured format

    The response format depends on the step configuration:

    **JSON Format** (`Content-Type: application/json`):

    Returns newline-delimited JSON (each line is a valid JSON object):

    ```json
    {"customer_id":"CUST001234","name":"John Smith","email":"john.smith@example.com","age":34}
    {"customer_id":"CUST001235","name":"Jane Doe","email":"jane.doe@example.com","age":28}
    {"customer_id":"CUST001236","name":"Bob Johnson","email":"bob.johnson@example.com","age":42}
    ```

    **CSV Format** (`Content-Type: text/csv; charset=UTF-8`):

    Returns CSV data with headers:

    ```csv
    customer_id,name,email,age
    CUST001234,John Smith,john.smith@example.com,34
    CUST001235,Jane Doe,jane.doe@example.com,28
    CUST001236,Bob Johnson,bob.johnson@example.com,42
    ```

    **Error Response:** `400 Bad Request`

    ```json
    {
      "code": "STEP_SAMPLE_ERROR",
      "message": "Step 'nonexistent_step' not found in any task. Available steps: customer_details, customer_addresses, account_info, transaction_data",
      "details": "IllegalArgumentException"
    }
    ```

    !!! note "Transformation Support"
    
        If the step has a [transformation](generator/transformation.md) configured, it will be automatically applied to the sample data before returning. This allows you to preview transformed output through the API.
        
        ```yaml
        # Example step with transformation
        steps:
          - name: "transactions"
            type: "json"
            transformation:
              className: "com.example.JsonArrayWrapperTransformer"
              mode: "whole-file"
        ```

    !!! tip "Direct Step Access"

        This endpoint is perfect for:

        - **Quick Testing**: Generate samples from specific steps without knowing which task contains them
        - **Step Validation**: Test individual step configurations in isolation
        - **Integration**: Get data directly in the step's native format for use in other systems
        - **Debugging**: Isolate and test problematic step configurations

        The step name must be unique across all tasks. If multiple tasks contain steps with the same name, the first one found will be used.

    **Example:**

    === "curl"

        ```bash
        # Generate sample from step with default parameters
        curl -X GET http://localhost:9898/sample/steps/customer_details

        # Generate with custom sample size and fast mode disabled 
        curl -X GET "http://localhost:9898/sample/steps/customer_details?sampleSize=15&fastMode=false"

        # Generate CSV data from step
        curl -X GET "http://localhost:9898/sample/steps/customer_csv_step?sampleSize=20" -o customers.csv

        # Generate large JSON sample
        curl -X GET "http://localhost:9898/sample/steps/transaction_data?sampleSize=100" -o transactions.json
        ```

    === "wget"

        ```bash
        # Generate sample from step with default parameters
        wget -O - http://localhost:9898/sample/steps/customer_details

        # Generate with custom parameters and save to file
        wget -O customer_sample.json "http://localhost:9898/sample/steps/customer_details?sampleSize=15"

        # Generate CSV data
        wget -O customers.csv "http://localhost:9898/sample/steps/customer_csv_step?sampleSize=50"
        ```

??? abstract "Generate Sample from Plan Task - `GET /sample/plans/{planName}/tasks/{taskName}`"

    Generate sample data from a specific task within a saved plan. If the task contains multiple steps, all steps will be included in the response.

    **Path Parameters:**

    - `planName` - The name of the saved plan
    - `taskName` - The name of the task within the plan

    **Query Parameters:**

    - `sampleSize` (optional, default: 10) - Number of sample records to generate per step
    - `fastMode` (optional, default: true) - Enable fast generation mode

    **Response:** `200 OK` (MultiSchemaSampleResponse)

    Returns a JSON object with samples for all steps within the specified task.

    ```json
    {
      "success": true,
      "executionId": "f2a8b1c4",
      "samples": {
        "customer-plan/accounts_step": [
          {
            "account_id": "ACC1234567890",
            "customer_id": "CUST000123",
            "balance": 5432.10,
            "status": "active",
            "opened_date": "2023-05-15"
          },
          {
            "account_id": "ACC9876543210",
            "customer_id": "CUST000456",
            "balance": 12500.50,
            "status": "active",
            "opened_date": "2023-08-22"
          },
          {
            "account_id": "ACC5555555555",
            "customer_id": "CUST000789",
            "balance": 825.75,
            "status": "frozen",
            "opened_date": "2024-01-10"
          }
        ],
        "customer-plan/transactions_step": [
          {
            "transaction_id": "TXN001",
            "account_id": "ACC1234567890",
            "amount": 150.00,
            "type": "debit",
            "timestamp": "2024-01-15T14:30:00Z"
          },
          {
            "transaction_id": "TXN002",
            "account_id": "ACC9876543210",
            "amount": 2500.00,
            "type": "credit",
            "timestamp": "2024-01-15T15:45:00Z"
          }
        ]
      },
      "metadata": {
        "sampleSize": 3,
        "actualRecords": 5,
        "generatedInMs": 180,
        "fastModeEnabled": true
      }
    }
    ```

    **Error Response:** `400 Bad Request`

    ```json
    {
      "code": "GENERATION_ERROR",
      "message": "Step/Task 'invalid_task' not found in plan. Available: json_account_file, csv_customer_file",
      "details": "IllegalArgumentException"
    }
    ```

    **Example:**

    === "curl"

        ```bash
        curl -X GET "http://localhost:9898/sample/plans/customer-plan/tasks/json_account_file?sampleSize=5"
        ```

    === "wget"

        ```bash
        wget -O - "http://localhost:9898/sample/plans/customer-plan/tasks/json_account_file?sampleSize=5"
        ```

??? abstract "Generate Sample from Plan Step - `GET /sample/plans/{planName}/tasks/{taskName}/steps/{stepName}`"

    Generate sample data from a specific step within a task in a saved plan. Returns **raw data** in the step's configured format (JSON, CSV, Parquet, etc.), making it ideal for integration with other systems or quick data inspection.

    **Path Parameters:**

    - `planName` - The name of the saved plan
    - `taskName` - The name of the task within the plan
    - `stepName` - The name of the step within the task

    **Query Parameters:**

    - `sampleSize` (optional, default: 10) - Number of sample records to generate
    - `fastMode` (optional, default: true) - Enable fast generation mode
    - `enableRelationships` (optional, default: false) - Enable relationship-based data generation

    **Response:** `200 OK` - Returns **raw data** in the step's configured format

    The response format depends on the step configuration:

    **JSON Format** (`Content-Type: application/json`):

    Returns newline-delimited JSON (each line is a valid JSON object):

    ```json
    {"account_id":"ACC1234567890","balance":5432.10,"status":"active","opened_date":"2023-05-15"}
    {"account_id":"ACC9876543210","balance":12500.50,"status":"active","opened_date":"2023-08-22"}
    {"account_id":"ACC5555555555","balance":825.75,"status":"frozen","opened_date":"2024-01-10"}
    ```

    **CSV Format** (`Content-Type: text/csv; charset=UTF-8`):

    Returns CSV data with headers:

    ```csv
    account_id,balance,status,opened_date
    ACC1234567890,5432.10,active,2023-05-15
    ACC9876543210,12500.50,active,2023-08-22
    ACC5555555555,825.75,frozen,2024-01-10
    ```

    **Parquet/ORC Format** (`Content-Type: application/octet-stream`):

    Returns binary data that can be written to a file and read by Spark/other tools.
    
    !!! note "Transformation Support"
    
        All sample endpoints support [transformations](generator/transformation.md). If a step has a transformation configured, it will be automatically applied before returning the sample data.

    **Error Response:** `400 Bad Request`

    ```json
    {
      "success": false,
      "executionId": "err12345",
      "error": {
        "code": "GENERATION_ERROR",
        "message": "Step 'invalid_step' not found in task 'json_account'. Available steps: file_account, validation_step",
        "details": "IllegalArgumentException"
      }
    }
    ```

    !!! tip "Working with Raw Data"

        This endpoint returns raw data without the JSON envelope, making it perfect for:

        - **Testing**: Validate your data generation configuration before running full plans
        - **Integration**: Pipe output directly to other tools or scripts
        - **Inspection**: Quick preview of generated data in native format
        - **Data Seeding**: Use generated data directly in your applications

        For JSON format, the output is newline-delimited JSON (NDJSON), where each line is a complete JSON object. This is the standard output format for Spark DataFrames written as JSON.

    **Example:**

    === "curl"

        ```bash
        # Get JSON sample data (newline-delimited JSON)
        curl -X GET "http://localhost:9898/sample/plans/account-plan/tasks/json_account/steps/file_account?sampleSize=3"

        # Get CSV sample data with fast mode disabled
        curl -X GET "http://localhost:9898/sample/plans/csv-plan/tasks/csv_task/steps/csv_step?sampleSize=10&fastMode=false"

        # Save JSON sample to file
        curl -X GET "http://localhost:9898/sample/plans/account-plan/tasks/json_account/steps/file_account?sampleSize=100" \
          -o sample_accounts.json

        # Save CSV sample to file
        curl -X GET "http://localhost:9898/sample/plans/customer-plan/tasks/csv_task/steps/customer_step?sampleSize=50" \
          -o sample_customers.csv
        ```

    === "wget"

        ```bash
        # Get sample data and save to file
        wget -O sample.json "http://localhost:9898/sample/plans/account-plan/tasks/json_account/steps/file_account?sampleSize=3"

        # Get CSV data and save
        wget -O customers.csv "http://localhost:9898/sample/plans/customer-plan/tasks/csv_task/steps/customer_step?sampleSize=20"
        ```

??? abstract "Generate Sample from Schema - `POST /sample/schema`"

    Generate sample data based on inline field definitions. This endpoint is ideal for ad-hoc testing, prototyping schemas, or generating sample data without creating task/plan files.

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
      "format": "json",
      "sampleSize": 5,
      "fastMode": true
    }
    ```

    **Parameters:**

    - `fields` (required) - Array of field definitions with name, type, and optional configuration
    - `format` (optional, default: "json") - Output format: "json", "csv", "parquet", "orc"
    - `sampleSize` (optional, default: 10) - Number of records to generate (max: 100)
    - `fastMode` (optional, default: true) - Enable fast generation mode

    **Response:** `200 OK` - Returns **raw data** in the specified format

    **JSON Format Response:**

    ```json
    {"account_id":"ACC1234567890","balance":5432.10,"customer_name":"John Smith"}
    {"account_id":"ACC9876543210","balance":7821.45,"customer_name":"Jane Doe"}
    {"account_id":"ACC5555555555","balance":9234.67,"customer_name":"Bob Johnson"}
    {"account_id":"ACC7777777777","balance":3456.89,"customer_name":"Alice Williams"}
    {"account_id":"ACC3333333333","balance":6789.12,"customer_name":"Charlie Brown"}
    ```

    **CSV Format Response:**

    ```csv
    account_id,balance,customer_name
    ACC1234567890,5432.10,John Smith
    ACC9876543210,7821.45,Jane Doe
    ACC5555555555,9234.67,Bob Johnson
    ACC7777777777,3456.89,Alice Williams
    ACC3333333333,6789.12,Charlie Brown
    ```

    **Error Response:** `400 Bad Request`

    ```json
    {
      "success": false,
      "executionId": "err98765",
      "error": {
        "code": "INVALID_SCHEMA",
        "message": "Field type 'invalid_type' is not supported. Use: string, int, long, double, boolean, timestamp, date, etc.",
        "details": "IllegalArgumentException"
      }
    }
    ```

    !!! tip "Use Cases"

        The `/sample/schema` endpoint is perfect for:

        - **Quick Prototyping**: Test field configurations without creating YAML files
        - **API Integration**: Generate mock data for testing APIs and services
        - **Schema Validation**: Verify field types and constraints before using in plans
        - **Documentation**: Generate sample data for API documentation and examples
        - **Data Exploration**: Experiment with different field generators and options

    **Example:**

    === "curl"

        ```bash
        # Generate JSON sample data
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
            "format": "json",
            "sampleSize": 5,
            "fastMode": true
          }'

        # Generate CSV sample data
        curl -X POST http://localhost:9898/sample/schema \
          -H "Content-Type: application/json" \
          -d '{
            "fields": [
              {
                "name": "user_id",
                "type": "long"
              },
              {
                "name": "email",
                "type": "string",
                "options": {
                  "expression": "#{Internet.emailAddress}"
                }
              },
              {
                "name": "age",
                "type": "int",
                "options": {
                  "min": 18,
                  "max": 80
                }
              }
            ],
            "format": "csv",
            "sampleSize": 10,
            "fastMode": true
          }' \
          -o users.csv
        ```

    === "wget"

        ```bash
        # Generate JSON sample data
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
            "format": "json",
            "sampleSize": 5,
            "fastMode": true
          }' \
          -O - http://localhost:9898/sample/schema

        # Generate CSV and save to file
        wget --method=POST \
          --header="Content-Type: application/json" \
          --body-data='{
            "fields": [
              {
                "name": "user_id",
                "type": "long"
              },
              {
                "name": "email",
                "type": "string"
              }
            ],
            "format": "csv",
            "sampleSize": 20,
            "fastMode": true
          }' \
          -O users.csv http://localhost:9898/sample/schema
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
- **Sample Generation**: Use `/sample/tasks/{taskName}`, `/sample/steps/{stepName}`, or `/sample/schema` for quick data previews
- **Monitoring**: Use `/run/history` and `/run/status/{id}` to track execution progress
- **Reports**: Use `/report/{runId}/{resource}` to access generated reports

This API provides comprehensive access to Data Caterer's functionality, allowing you to integrate data generation capabilities into your applications and workflows.
