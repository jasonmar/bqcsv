#!/bin/bash
set -e
set -x

CP='target/scala-2.13/bqcsv_2.13-0.1.0-SNAPSHOT.jar:target/scala-2.13/bqcsv.dep.jar'
java -cp "$CP" com.google.cloud.imf.BqCsv \
  --replace \
  --delimiter 'þ' \
  --external \
  --dataset dataset \
  --project project \
  --templateTableSpec project:dataset.template \
  src/test/resources/sample1.txt \
  gs://project-test/example1 \
  project:dataset.table
