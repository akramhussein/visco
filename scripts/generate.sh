#!/bin/bash
$SCRIPTS_HOME/check_env.sh
if [ $? -ne 0 ]
then 
    echo "Environment not configured properly. Check env.sh and source it."
    exit 1
fi

SLAVES=`cat $HADOOP_HOME/conf/all_slaves | wc -l`

count=$SLAVES

CLIENT=`cat $HADOOP_HOME/conf/client.txt`

echo "Generating $CHUNKS chunks of text..."
$SCRIPTS_HOME/generate_text.sh $CHUNKS 1

echo "Done"
echo "Getting file..."
oarsh $CLIENT "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/hadoop fs -get $HFILE $FILE"
echo "Done"


