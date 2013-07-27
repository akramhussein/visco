#!/bin/bash                                                                                                         

# arguments: no of deployed tasktrackers
if [ $# -ne 3 ]
then 
    echo "Required arguments : <user> <reservationId> <site>"
    exit 1
fi

USER=$1
JOB_ID=$2
CLUSTER=$3

echo $USER" : reservation_id "$JOB_ID

# copy the key to the keys dir and setting permissions
if [ ! -d $HOME/keys ]
then 
    echo $HOME/keys" not found, creating it."
    mkdir $HOME/keys
else
    rm $HOME/keys/*
fi

cp /tmp/oargrid/oargrid_ssh_key_${USER}_${JOB_ID} $HOME/keys
chmod +r $HOME/keys/oargrid_ssh_key_${USER}_${JOB_ID}

# update the VARIABLES for the rest of the scripts
# it is done this way so that we do not have to change the 
# remaining scripts.

HADOOP_HOME=$HOME/visco/hadoop-1.0.1

ENV_VARS="export OAR_JOB_ID=$JOB_ID\n\
export OAR_JOB_KEY_FILE=$HOME/keys/oargrid_ssh_key_${USER}_${JOB_ID}\n\
export HADOOP_HOME=$HADOOP_HOME\n\
export SCRIPTS_HOME=$HADOOP_HOME/scripts\n\
export EXAMPLE_JAR=$HADOOP_HOME/hadoop-examples-1.0.2-SNAPSHOT.jar\n\
export APPS_LOGS=$HADOOP_HOME/benchmark/apps\n\
export ALL_NODES=$HADOOP_HOME/conf/nodes.txt\n\
export HADOOP_VERSION=1.0.1\n\
export USER=$USER\n\
export CLUSTER=$CLUSTER\n\n\
# the local file with the input\n\
export FILE=/home/kkloudas/fis\n\n\
# the file in the HDFS (when transfered and waiting to be processed)\n\
export HFILE=/fis\n\n\
# the output dir of the job in HDFS\n\
export OUTPUT=/output\n\n\
# the number of chunks for the generator\n\
export CHUNKS=10"

echo -e $ENV_VARS > env.sh
#cat env.sh
# check if we need to run again the env.sh given that we already have exported the vars. TODO
source env.sh

# run the auto-config
$SCRIPTS_HOME/auto-config.sh
$SCRIPTS_HOME/check_env.sh
if [ $? -ne 0 ]
then
    echo "Environment not configured properly. Check env.sh and source it."
    exit 1
fi

JOBTRACKER=`cat $HADOOP_HOME/conf/jobtracker.txt`
CLIENT=`cat $HADOOP_HOME/conf/client.txt`

# kill any possible existing instance of hadoop
oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/stop-mapred.sh"
oarsh $JOBTRACKER "killall java"
sleep 10

# start HDFS
$SCRIPTS_HOME/hdfs-deploy.sh

# start a new hadoop instance
oarsh $JOBTRACKER "source $SCRIPTS_HOME/env.sh; $HADOOP_HOME/bin/start-mapred.sh"
echo "Deployment done"
sleep 30



