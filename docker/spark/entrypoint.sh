#!/bin/bash
set -e

SPARK_HOME="${SPARK_HOME:-/opt/spark}"

case "${SPARK_MODE:-master}" in
  master)
    # Start History Server in background (port 18080)
    if [ -n "$SPARK_HISTORY_OPTS" ]; then
      echo "Starting Spark History Server (background)..."
      export SPARK_HISTORY_OPTS
      "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer &
    fi
    echo "Starting Spark Master..."
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST:-0.0.0.0}" \
      --port "${SPARK_MASTER_PORT:-7077}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
    ;;

  worker)
    MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
    WORKER_ARGS=""
    [ -n "${SPARK_WORKER_MEMORY}" ]   && WORKER_ARGS="$WORKER_ARGS --memory ${SPARK_WORKER_MEMORY}"
    [ -n "${SPARK_WORKER_CORES}" ]    && WORKER_ARGS="$WORKER_ARGS --cores ${SPARK_WORKER_CORES}"
    [ -n "${SPARK_WORKER_WEBUI_PORT}" ] && WORKER_ARGS="$WORKER_ARGS --webui-port ${SPARK_WORKER_WEBUI_PORT}"
<<<<<<< HEAD
    echo "Starting Spark Worker ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ $MASTER_URL"
=======
    echo "Starting Spark Worker Ã¢â€ â€™ $MASTER_URL"
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      $WORKER_ARGS "$MASTER_URL"
    ;;

  history)
    echo "Starting Spark History Server..."
    export SPARK_HISTORY_OPTS="${SPARK_HISTORY_OPTS:--Dspark.history.fs.logDirectory=/opt/spark-events}"
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;

  *)
    exec "$@"
    ;;
esac
