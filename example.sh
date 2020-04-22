#!/bin/bash
set -e
set -x

lib='target/scala-2.13'
version='0.3.8-SNAPSHOT'
depversion='0.3.8-SNAPSHOT'
project='project'
bucket='bucket'

CP="$lib/open-systems-connector_2.13-${version}.jar:$lib/open-systems-connector-assembly-${depversion}-deps.jar"

java -cp "$CP" com.google.cloud.imf.OSC \
  --replace \
  --debug \
  --dataset dataset \
  --project $project \
  src/test/resources/sample2.txt \
  gs://$bucket/sample2 \
  $project:dataset.test_table_2
