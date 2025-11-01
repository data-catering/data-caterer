#!/usr/bin/env bash
DATA_CATERER_MASTER="${DATA_CATERER_MASTER:-local[*]}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
JAVA_OPTS="-Dlog4j.configurationFile=file:///opt/app/log4j2.properties -Djdk.module.illegalAccess=deny -Djava.security.manager=allow"
JAVA_17_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
SPARK_OPTS="$ADDITIONAL_OPTS --conf \"spark.driver.extraJavaOptions=$JAVA_OPTS $JAVA_17_OPTS\" --conf \"spark.executor.extraJavaOptions=$JAVA_OPTS $JAVA_17_OPTS\""
JAVA_CLASSPATH="/opt/app/jars/*:/opt/app/data-caterer-api.jar:/opt/app/data-caterer.jar:/opt/app/custom/*"

# Pre and post processor script configuration
PRE_PROCESSOR_SCRIPT="${PRE_PROCESSOR_SCRIPT:-}"
POST_PROCESSOR_SCRIPT="${POST_PROCESSOR_SCRIPT:-}"
POST_PROCESSOR_CONDITION="${POST_PROCESSOR_CONDITION:-success}"  # success, failure, always

# Function to run processor scripts with error handling
run_processor_script() {
  local script_path="$1"
  local script_type="$2"
  
  if [ -n "$script_path" ] && [ -f "$script_path" ]; then
    echo "Running $script_type processor script: $script_path"
    if bash "$script_path"; then
      echo "$script_type processor script completed successfully"
      return 0
    else
      echo "ERROR: $script_type processor script failed with exit code $?"
      return 1
    fi
  elif [ -n "$script_path" ]; then
    echo "WARNING: $script_type processor script specified but not found: $script_path"
    return 1
  fi
  return 0
}

# Function to determine if post processor should run
should_run_post_processor() {
  local data_caterer_exit_code="$1"
  
  case "$POST_PROCESSOR_CONDITION" in
    "success")
      [ "$data_caterer_exit_code" -eq 0 ]
      ;;
    "failure")
      [ "$data_caterer_exit_code" -ne 0 ]
      ;;
    "always")
      true
      ;;
    *)
      echo "WARNING: Unknown POST_PROCESSOR_CONDITION '$POST_PROCESSOR_CONDITION', defaulting to 'success'"
      [ "$data_caterer_exit_code" -eq 0 ]
      ;;
  esac
}

if [ -e /opt/app/job.jar ]; then
  echo "Job jar found. Adding to java classpath"
  JAVA_CLASSPATH+=":/opt/app/job.jar"
fi

# Run pre-processor script if specified
if ! run_processor_script "$PRE_PROCESSOR_SCRIPT" "pre"; then
  echo "Pre-processor script failed, exiting..."
  exit 1
fi

if [[ "$DEPLOY_MODE" == "standalone" ]] ; then
  echo "Running Data Caterer as a standalone application"
  CMD=(
    java
    "$JAVA_OPTS $JAVA_17_OPTS"
    -cp "$JAVA_CLASSPATH"
    io.github.datacatering.datacaterer.core.ui.DataCatererUI
  )
else
  echo "Running Data Caterer as a Spark job"
  CMD=(
    java
    "$JAVA_OPTS $JAVA_17_OPTS"
    -cp "$JAVA_CLASSPATH"
    io.github.datacatering.datacaterer.App
  )
fi

# Execute Data Caterer and capture exit code
eval "${CMD[@]}"
DATA_CATERER_EXIT_CODE=$?

# Run post-processor script based on condition
if should_run_post_processor "$DATA_CATERER_EXIT_CODE"; then
  echo "Data Caterer completed with exit code $DATA_CATERER_EXIT_CODE, running post-processor..."
  if ! run_processor_script "$POST_PROCESSOR_SCRIPT" "post"; then
    echo "Post-processor script failed, but Data Caterer exit code will be preserved"
  fi
else
  echo "Post-processor conditions not met (exit code: $DATA_CATERER_EXIT_CODE, condition: $POST_PROCESSOR_CONDITION)"
fi

# Exit with Data Caterer's original exit code
exit $DATA_CATERER_EXIT_CODE
