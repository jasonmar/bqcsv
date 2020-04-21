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

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}

import com.google.cloud.imf.osc.Decoders.{DecimalDecoder, StringDecoder, TimestampDecoder, TimestampDecoder2}
import org.scalatest.flatspec.AnyFlatSpec

class DecoderSpec extends AnyFlatSpec {
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
      ("2020-04-17 12:08:57+00:00",12),
      ("2020-04-17 12:08:57+01:00",11),
      ("2020-04-17 12:08:57-07:00",19),
      ("2020-04-17 12:08:57-08:00",20)
    )
    val fmt = DateTimeFormatter.ofPattern(Decoders.OffsetFormat)
    for (e <- example){
      val timestamp = OffsetDateTime.from(fmt.parse(e._1))
      val utcTimeStamp = timestamp.atZoneSameInstant(Decoders.UTC)
      System.out.println(timestamp.getOffset)
      System.out.println(utcTimeStamp.getOffset)
      System.out.println(timestamp.getHour)
      System.out.println(utcTimeStamp.getHour)
      assert(utcTimeStamp.getHour == e._2)
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
      System.out.println(timestamp.getOffset)
      System.out.println(timestamp.getHour)
      System.out.println(utcTimestamp.getOffset)
      System.out.println(utcTimestamp.getHour)
      assert(utcTimestamp.getHour == e._2)
    }
  }

  it should "parse schema" in {
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
