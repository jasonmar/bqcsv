#!/bin/bash

CP='target/scala-2.13/bqcsv_2.13-0.1.0-SNAPSHOT.jar:target/scala-2.13/bqcsv.dep.jar'
java -cp "$CP" com.google.cloud.imf.BqCsv \
  --replace \
  --delimiter 'þ' \
  --schema 'key1:STRING:24,key2:STRING:24,key3:STRING:24,key4:STRING:24,STATUS:STRING:15,date1:TIMESTAMP,qty1:NUMERIC:14.4,key5:STRING:24,key6:STRING:24,qty2:NUMERIC:14.4,date2:TIMESTAMP,key7:STRING:24,key8:STRING:24,timestamp1:TIMESTAMP,timestamp2:TIMESTAMP,id1:STRING:40,id2:STRING:40,id3:STRING:40,id4:STRING:40,id5:NUMERIC:5.0,rank:TIMESTAMP:6' \
  --dataset dataset \
  --project project \
  src/test/resources/sample1.txt \
  gs://bucket/example1 \
  project:dataset.table1
