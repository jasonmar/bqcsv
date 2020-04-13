#!/bin/bash
set -e
set -x

lib='target/scala-2.13'

CP="$lib/open-systems-connector_2.13-0.3.0-SNAPSHOT.jar:$lib/open-systems-connector-assembly-0.3.0-SNAPSHOT-deps.jar"

java -cp "$CP" com.google.cloud.imf.OSC \
  --help \
  --replace \
  --autodetect \
  --debug \
  --dataset dataset \
  --project project \
  src/test/resources/sample1.txt \
  gs://bucket/example2 \
  project:dataset.table
