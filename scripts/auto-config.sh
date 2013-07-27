#!/bin/bash
$SCRIPTS_HOME/check_env.sh
if [ $? -ne 0 ]
then
    echo "Environment is not configured properly. Check env.sh and source it."
    exit 1
fi

CONF_DIR=$HADOOP_HOME/conf

oargridstat -l $OAR_JOB_ID | sort | uniq > $CONF_DIR/ttemp.txt

taktuk -c oarsh -s -f $CONF_DIR/ttemp.txt broadcast exec [ "$SCRIPTS_HOME/cleanup.sh; test -e $HADOOP_HOME" ] | grep "status 0" | cut -d '.' -f 1 | $SCRIPTS_HOME/get_full_name.sh > $CONF_DIR/temp.txt

LINES=`cat $CONF_DIR/temp.txt | wc -l`
cp $CONF_DIR/temp.txt $ALL_NODES

tail -n `expr $LINES - 2` $ALL_NODES >$CONF_DIR/machines.txt
head -n 1 $ALL_NODES >$CONF_DIR/jobtracker.txt
head -n 2 $ALL_NODES | tail -n 1 >$CONF_DIR/client.txt

echo Checking reservation $OAR_JOB_ID: $LINES machines out of `cat $CONF_DIR/ttemp.txt | grep '.' | wc -l` are configured properly and usable.
rm -rf $CONF_DIR/temp.txt $CONF_DIR/ttemp.txt

# Hadoop configuration
tail -n 1 $CONF_DIR/machines.txt  > $CONF_DIR/masters
head -n -1 $CONF_DIR/machines.txt > $CONF_DIR/all_slaves

cp $CONF_DIR/all_slaves $CONF_DIR/slaves

MASTER=`cat $CONF_DIR/masters`
JOBTRACKER=`cat $CONF_DIR/jobtracker.txt`
perl -pi -e "s/[-\.\w]*:9000/${MASTER}:9000/" $HADOOP_HOME/conf/core-site.xml
perl -pi -e "s/[-\.\w]*:9000/${JOBTRACKER}:9000/" $HADOOP_HOME/conf/mapred-site.xml

# Other cleanup: remove ~/.ssh/known_hosts on all nodes
taktuk -s -c oarsh -f $ALL_NODES broadcast exec [ "rm -rf $HOME/.ssh/known_hosts" ]
