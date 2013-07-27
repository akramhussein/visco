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
sleep 10

$SCRIPTS_HOME/hdfs-deploy.sh

oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/start-mapred.sh"
echo "Deployment done"
sleep 30

mkdir -p $APPS_LOGS/sort_logs

oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/hadoop fs -put $FILE $HFILE"

oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/hadoop fs -rmr $HFILE/_logs; $HADOOP_HOME/bin/hadoop fs -ls $HFILE"

sleep 10

echo "Starting wordCount..."
oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/hadoop jar $EXAMPLE_JAR wordcount $HFILE $OUTPUT &> $APPS_LOGS/sort_logs/wordcount-$1-tts.log"
echo "Done"

