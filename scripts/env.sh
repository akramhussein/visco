export OAR_JOB_ID=34324
export OAR_JOB_KEY_FILE=/home/kkloudas/keys/oargrid_ssh_key_kkloudas_34324
export HADOOP_HOME=/home/kkloudas/hadoop-1.0.1
export SCRIPTS_HOME=/home/kkloudas/hadoop-1.0.1/scripts
export EXAMPLE_JAR=/home/kkloudas/hadoop-1.0.1/hadoop-examples-1.0.2-SNAPSHOT.jar
export APPS_LOGS=/home/kkloudas/hadoop-1.0.1/benchmark/apps
export ALL_NODES=/home/kkloudas/hadoop-1.0.1/conf/nodes.txt
export HADOOP_VERSION=1.0.1
export USER=kkloudas
export CLUSTER=sophia

# the local file with the input
export FILE=/home/kkloudas/fis

# the file in the HDFS (when transfered and waiting to be processed)
export HFILE=/fis

# the output dir of the job in HDFS
export OUTPUT=/output

# the number of chunks for the generator
export CHUNKS=10
