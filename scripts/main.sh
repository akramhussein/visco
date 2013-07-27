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

#$SCRIPTS_HOME/dist_grep.sh $count
#sleep 10

$SCRIPTS_HOME/dist_grep.sh hdfs $count
sleep 10

exit 0
