#!/usr/bin/env bash
DATA_CATERER_MASTER="${DATA_CATERER_MASTER:-local[*]}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
JAVA_OPTS="-Dlog4j.configurationFile=file:///opt/app/log4j2.properties -Djdk.module.illegalAccess=deny"
JAVA_17_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
ALL_OPTS="$ADDITIONAL_OPTS --conf \"spark.driver.extraJavaOptions=$JAVA_OPTS\" --conf \"spark.executor.extraJavaOptions=$JAVA_OPTS\""

if [[ "$DEPLOY_MODE" -eq "standalone" ]] ; then
  echo "Running Data Caterer as a standalone application"
  CMD=(
    java
    "$JAVA_OPTS"
    "$JAVA_17_OPTS"
    -cp "/opt/spark/jars/*:/opt/app/job.jar"
    io.github.datacatering.datacaterer.core.ui.DataCatererUI
  )
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
fi

eval "${CMD[@]}"
