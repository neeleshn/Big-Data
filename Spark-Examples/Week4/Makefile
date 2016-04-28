#!/bin/bash
# Author: Nat Tuck

jar:
	sbt package
	cp target/scala-*/demo_*.jar Demo.jar

run: jar
	rm -rf out
	sbt run

prof: jar
	rm -rf out
	SBT_OPTS="-Xprof" sbt run

hadoop: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar
	hadoop fs -get /user/neeleshganan/out .

clean: hstop
	rm -rf project target *.jar out

format:
	rm -rf /tmp/*
	hdfs namenode -format

hstart: format
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop: 
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

hcopy: hstart
	hadoop fs -mkdir -p /user/neeleshganan
	hadoop fs -put baseball /user/neeleshganan/

