/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.imf.osc

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}

import com.google.cloud.bigquery.{Field, FieldList, Schema, StandardSQLTypeName}
import com.google.cloud.imf.osc.Decoders.{DecimalDecoder, StringDecoder, TimestampDecoder, TimestampDecoder2}
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import org.scalatest.flatspec.AnyFlatSpec

class DecoderSpec extends AnyFlatSpec {
  Util.configureLogging(true)
  System.setProperty("user.timezone", "Etc/UTC");

  "decoder" should "parse timestamp" in {
    val pattern = "yyyy-MM-dd HH:mm:ssz"
    val pattern2 = Decoders.LocalFormat
    val example = "2006-01-02 03:04:05+00:00"
    val example2 = "2006-01-02 03:04:05"
    val fmt = DateTimeFormatter.ofPattern(pattern)
    val fmt2 = DateTimeFormatter.ofPattern(pattern2)
    val t = ZonedDateTime.from(fmt.parse(example))
    val t2 = LocalDateTime.from(fmt2.parse(example2)).atZone(ZoneId.of("Etc/GMT"))
    assert(t.toEpochSecond == t2.toEpochSecond)
    assert(t.getNano == 0)
    assert(t2.getNano == 0)
  }

  "timestamp decoder" should "offset" in {
    val example = Seq(
      ("2020-04-17 12:08:57+00:00",12,12),
      ("2020-04-17 12:08:57+01:00",12,11),
      ("2020-04-17 12:08:57-00:00",12,12),
      ("2020-04-17 12:08:57-01:00",12,13),
      ("2020-04-17 12:08:57-02:00",12,14),
      ("2020-04-17 12:08:57-03:00",12,15),
      ("2020-04-17 12:08:57-04:00",12,16),
      ("2020-04-17 12:08:57-05:00",12,17),
      ("2020-04-17 12:08:57-06:00",12,18),
      ("2020-04-17 12:08:57-07:00",12,19),
      ("2020-04-17 12:08:57-08:00",12,20)
    )
    val fmt = DateTimeFormatter.ofPattern(Decoders.OffsetFormat)
    for (e <- example){
      val timestamp = OffsetDateTime.from(fmt.parse(e._1))
      val utcTimeStamp = timestamp.atZoneSameInstant(Decoders.UTC)

      System.out.println(s"${e._1} ${Timestamp.valueOf(timestamp.toLocalDateTime)} ${timestamp.toInstant.getEpochSecond / 3600} ${timestamp.toEpochSecond / 3600} ${utcTimeStamp.getHour}")
      assert(timestamp.getHour == e._2)
      assert(utcTimeStamp.getHour == e._3)
    }
  }

  it should "zone2" in {
    val sp = new TableSchemaProvider(Schema.of(FieldList.of(Seq[Field](
      Field.of("a", StandardSQLTypeName.STRING),
      Field.of("b", StandardSQLTypeName.TIMESTAMP),
      Field.of("c", StandardSQLTypeName.TIMESTAMP)
    ):_*)), "Etc/Utc")
    val example = TestUtil.resource("sample2.txt")
    val decoders = sp.decoders
    val cols = decoders.map(_.columnVector(12))
    val lines = example.linesIterator.toArray
    var i = 0
    var j = 0
    while (j < lines.length) {
      val fields = lines(j).split('þ')
      i = 0
      while (i < decoders.length) {
        val decoder = decoders(i)
        val col = cols(i)
        decoder.get(fields(i), col, j)
        (decoder,col) match {
          case (_: TimestampDecoder, x: TimestampColumnVector) if i == 1 =>
            val t = x.time(j)
            val epochHour = t/3600000
            val ts = new Timestamp(t)
            val msg = s"$i $j $epochHour ${ts.toString()}"
            System.out.println(msg)
            assert(epochHour == 440821 + j)
          case _ =>
        }
        i += 1
      }
      j += 1
    }
  }

  it should "zone" in {
    val example = Seq(
      ("2020-04-17 12:08:57 UTC+00:00",12),
      ("2020-04-17 12:08:57 UTC+01:00",11),
      ("2020-04-17 12:08:57 UTC-07:00",19),
      ("2020-04-17 12:08:57 UTC-08:00",20)
    )
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")
    for (e <- example){
      val timestamp = ZonedDateTime.from(fmt.parse(e._1))
      val utcTimestamp = timestamp.withZoneSameInstant(Decoders.UTC)
      assert(utcTimestamp.getHour == e._2)
    }
  }

  "StringToNum" should "decimal" in {
    // positive
    assert(StringToNum.decimalValue("1",5) == 100000L)
    assert(StringToNum.decimalValue("0.01",5) == 1000L)
    assert(StringToNum.decimalValue("0.00001",5) == 1L)

    // no leading zero
    assert(StringToNum.decimalValue(".01",5) == 1000L)
    assert(StringToNum.decimalValue(".00001",5) == 1L)
    assert(StringToNum.decimalValue(".",5) == 0L)

    // negative
    assert(StringToNum.decimalValue("-0.01",5) == -1000L)
    assert(StringToNum.decimalValue("-.01",5) == -1000L)
    assert(StringToNum.decimalValue("-1",5) == -100000L)
    assert(StringToNum.decimalValue("-.00001",5) == -1L)
    assert(StringToNum.decimalValue("-0.00001",5) == -1L)

    // excess scale
    assert(StringToNum.decimalValue("0.010001",5) == 1000L)
    assert(StringToNum.decimalValue(".010001",5) == 1000L)
    assert(StringToNum.decimalValue("1.000001",5) == 100000L)
    assert(StringToNum.decimalValue(".000011",5) == 1L)
    assert(StringToNum.decimalValue("0.000011",5) == 1L)
    assert(StringToNum.decimalValue("-0.010001",5) == -1000L)
    assert(StringToNum.decimalValue("-.010001",5) == -1000L)
    assert(StringToNum.decimalValue("-1.000001",5) == -100000L)
    assert(StringToNum.decimalValue("-.000011",5) == -1L)
    assert(StringToNum.decimalValue("-0.000011",5) == -1L)
  }

  it should "long" in {
    assert(StringToNum.longValue("1") == 1L)
    assert(StringToNum.longValue("11") == 11L)
    assert(StringToNum.longValue("111") == 111)
    assert(StringToNum.longValue("12345") == 12345L)
    assert(StringToNum.longValue("123456789") == 123456789L)
    assert(StringToNum.longValue("-1") == -1L)
    assert(StringToNum.longValue("-11") == -11L)
    assert(StringToNum.longValue("-111") == -111L)
    assert(StringToNum.longValue("-12345") == -12345L)
    assert(StringToNum.longValue("-123456789") == -123456789L)
  }

  "cli" should "parse schema" in {
    val example = "key1:STRING:24,key2:STRING:24,key3:STRING:24,key4:STRING:24,STATUS:STRING:15,date1:TIMESTAMP,qty1:NUMERIC:14.4,key5:STRING:24,key6:STRING:24,qty2:NUMERIC:14.4,date2:TIMESTAMP,key7:STRING:24,key8:STRING:24,timestamp1:TIMESTAMP,timestamp2:TIMESTAMP,id1:STRING:40,id2:STRING:40,id3:STRING:40,id4:STRING:40,id5:NUMERIC:5.0,rank:TIMESTAMP:America/Chicago"
    val sp = CliSchemaProvider(example)
    val expected = Array[Decoder](
      StringDecoder(24),
      StringDecoder(24),
      StringDecoder(24),
      StringDecoder(24),
      StringDecoder(15),
      TimestampDecoder(),
      DecimalDecoder(14,4),
      StringDecoder(24),
      StringDecoder(24),
      DecimalDecoder(14,4),
      TimestampDecoder(),
      StringDecoder(24),
      StringDecoder(24),
      TimestampDecoder(),
      TimestampDecoder(),
      StringDecoder(40),
      StringDecoder(40),
      StringDecoder(40),
      StringDecoder(40),
      DecimalDecoder(5,0),
      TimestampDecoder2(zoneId = "America/Chicago")).toSeq

    assert(sp.decoders.toSeq == expected)
  }
}
