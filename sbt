#!/bin/bash

root=$(
	cd $(dirname $(readlink $0 || echo $0))/..
	pwd
)

sbtjar=sbt-launch.jar

if [ ! -f $sbtjar ]; then
	echo "downloading $sbtjar" 1>&2
	curl -O http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.12.2-RC2/$sbtjar
fi

test -f $sbtjar || exit 1

sbtjar_md5=$(openssl md5 < $sbtjar|cut -f2 -d'='|awk '{print $1}')

if [ "${sbtjar_md5}" != db591e4ab57657591ae3ccc666f77b77 ]; then
	echo 'bad sbtjar!' 1>&2
	exit 1
fi

test -f ~/.sbtconfig && . ~/.sbtconfig

java -ea -server $SBT_OPTS $JAVA_OPTS			\
	-XX:+AggressiveOpts             		\
	-XX:+OptimizeStringConcat			\
	-XX:+UseConcMarkSweepGC               		\
	-XX:+CMSParallelRemarkEnabled   		\
	-XX:+CMSClassUnloadingEnabled   		\
	-XX:+CMSIncrementalMode         		\
	-Dio.netty.epollBugWorkaround=true		\
	-Xms128M					\
	-Xmx512M					\
	-jar $sbtjar "$@"

