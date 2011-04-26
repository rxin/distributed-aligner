#!/usr/bin/env bash

# Set Spark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_HOME, to point to your Mesos installation
# - SCALA_HOME, to point to your Scala installation
# - SPARK_CLASSPATH, to add elements to Spark's classpath
# - SPARK_JAVA_OPTS, to add JVM options
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.

export SPARK_MEM="5000m"

BASEDIR=/root
export JAVA_HOME=/usr/lib/jvm/java-6-sun
export SCALA_HOME=$BASEDIR/scala-2.8.1.final
export MESOS_HOME=$BASEDIR/mesos

JARS=
JARS+=$BASEDIR/spark/conf/
JARS+=:/nfs/spark/alignerscala_2.8.1-1.0.jar
JARS+=:$SCALA_HOME/lib/scala-library.jar:$SCALA_HOME/lib/scala-compiler.jar
export SPARK_CLASSPATH=$JARS

#export SPARK_JAVA_OPTS="-Dspark.default.parallelism=40 -Dspark.cache.class=spark.SerializingCache -Dspark.boundedMemoryCache.memoryFraction=0.50 -XX:+DoEscapeAnalysis -Dspark.task.cpus=1 -XX:NewRatio=7 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC"
#export SPARK_JAVA_OPTS="-Dspark.default.parallelism=40 -Dspark.cache.class=spark.SerializingCache -Dspark.boundedMemoryCache.memoryFraction=0.50 -XX:+DoEscapeAnalysis -Dspark.task.cpus=1 -XX:+UseCompressedOops"
#export SPARK_JAVA_OPTS="-Dspark.default.parallelism=40 -XX:+DoEscapeAnalysis -Dspark.task.cpus=2 -XX:+UseCompressedOops"
#export SPARK_JAVA_OPTS="-Dspark.default.parallelism=40 -XX:+DoEscapeAnalysis -Dspark.task.cpus=1 -XX:+UseCompressedOops -Dspark.serialization=spark.KryoSerialization -Dspark.cache.class=spark.SerializingCache -Dspark.boundedMemoryCache.memoryFraction=0.8 -Dspark.locality.wait=10000 -Dspark.kryo.registrator=SparseVectorOriginalRegistrator"
export SPARK_JAVA_OPTS="-Dspark.default.parallelism=4 -XX:+DoEscapeAnalysis -Dspark.task.cpus=1 -XX:+UseCompressedOops -Dspark.boundedMemoryCache.memoryFraction=0.8 -Dspark.locality.wait=10000"
#export SPARK_JAVA_OPTS="-Dspark.default.parallelism=40 -XX:+DoEscapeAnalysis -Dspark.task.cpus=2 -XX:+UseCompressedOops -Dspark.serialization=spark.KryoSerialization -Dspark.locality.wait=10000"
