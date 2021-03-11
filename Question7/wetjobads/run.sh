#!/usr/bin/env bash

sbt assembly && 
spark-submit \
--master local \
target/scala-2.11/wetjobads-assembly-0.1.0-SNAPSHOT.jar

