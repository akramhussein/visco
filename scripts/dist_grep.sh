#!/bin/bash

# arguments: no of deployed tasktrackers

$SCRIPTS_HOME/check_env.sh
if [ $? -ne 0 ]
then
    echo "Environment not configured properly. Check env.sh and source it."
    exit 1
fi

JOBTRACKER=`cat $HADOOP_HOME/conf/jobtracker.txt`
CLIENT=`cat $HADOOP_HOME/conf/client.txt`

oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/stop-mapred.sh"
oarsh $JOBTRACKER "killall java"
$SCRIPTS_HOME/hdfs-deploy.sh

oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/start-mapred.sh"
echo "Deployment done"
sleep 10

mkdir -p $APPS_LOGS/grep_logs

oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/hadoop fs -put $FILE $HFILE"

$HADOOP_HOME/bin/hadoop fs -rmr $HFILE/_logs
$HADOOP_HOME/bin/hadoop fs -ls $HFILE

sleep 5

echo "Starting grep..."
oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/hadoop jar $EXAMPLE_JAR grep $HFILE $OUTPUT 'a' &> $APPS_LOGS/grep_logs/grep-$1-tts.log"
echo "Done"


