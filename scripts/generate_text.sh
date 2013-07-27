#!/bin/bash

#arguments: no of chunks, no of parts

CLIENTS=$1
UNCOMPRESSED_DATA_BYTES=$((${CLIENTS} * 67108864))
NUM_MAPS=$2

JOBTRACKER=`cat $HADOOP_HOME/conf/jobtracker.txt`
CLIENT=`cat $HADOOP_HOME/conf/client.txt`
oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/stop-mapred.sh"
oarsh $JOBTRACKER "killall java"

$SCRIPTS_HOME/hdfs-deploy.sh

oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/start-mapred.sh"
echo "Deployment done"
sleep 10

mkdir -p $APPS_LOGS/random_text_logs

oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; ${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${UNCOMPRESSED_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${UNCOMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D test.randomtextwrite.min_words_key=10 \
  -D test.randomtextwrite.max_words_key=10 \
  -D test.randomtextwrite.min_words_value=100 \
  -D test.randomtextwrite.max_words_value=100 \
  -D mapred.output.compress=false \
  -outFormat org.apache.hadoop.mapred.TextOutputFormat\
  $HFILE &> $APPS_LOGS/random_text_logs/randomtextwrite-$1-chunks-$2-parts.log"

echo "Done"
