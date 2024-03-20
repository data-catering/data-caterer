#!/usr/bin/env bash
DATA_CATERER_MASTER="${DATA_CATERER_MASTER:-local[*]}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
JAVA_OPTS="-Dlog4j.configurationFile=file:///opt/app/log4j2.properties -Djdk.module.illegalAccess=deny"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
ALL_OPTS="$ADDITIONAL_OPTS --conf \"spark.driver.extraJavaOptions=$JAVA_OPTS\" --conf \"spark.executor.extraJavaOptions=$JAVA_OPTS\""

if [[ "$DEPLOY_MODE" -eq "standalone" ]] ; then
  echo "Running Data Caterer as a standalone application"
  java -cp "/opt/spark/jars/*:/opt/app/job.jar" io.github.datacatering.datacaterer.core.ui.DataCatererUI
else
  echo "Running Data Caterer as a Spark job"
  CMD=(
    /opt/spark/bin/spark-submit
    --class io.github.datacatering.datacaterer.App
    --master "$DATA_CATERER_MASTER"
    --deploy-mode "$DEPLOY_MODE"
    --driver-memory "$DRIVER_MEMORY"
    --executor-memory "$EXECUTOR_MEMORY"
    "$ALL_OPTS"
    file:///opt/app/job.jar
  )

  eval "${CMD[@]}"
fi

