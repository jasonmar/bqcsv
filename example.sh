#!/bin/bash
set -e
set -x

CP='target/scala-2.13/bqcsv_2.13-0.1.0-SNAPSHOT.jar:target/scala-2.13/bqcsv.dep.jar'
java -cp "$CP" com.google.cloud.imf.BqCsv \
  --replace \
  --autodetect \
  --dataset dataset \
  --project pso-wmt-dl \
  src/test/resources/sample1.txt \
  gs://pso-wmt-dl-test/example2 \
  pso-wmt-dl:dataset.table5
