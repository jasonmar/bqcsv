#!/bin/bash
set -e
set -x

lib='target/scala-2.13'
version=0.6.3-SNAPSHOT
depversion=0.6.3-SNAPSHOT
project=project
bucket=bucket
dataset=dataset

CP="$lib/open-systems-connector_2.13-${version}.jar:$lib/open-systems-connector-assembly-${depversion}-deps.jar"

java -cp "$CP" com.google.cloud.imf.OSC \
  --replace \
  --debug \
  --dataset ${dataset} \
  --project ${project} \
  gs://${bucket}/sample2 \
  ${project}:${dataset}.test_table_2 \
  src/test/resources/sample2.txt
