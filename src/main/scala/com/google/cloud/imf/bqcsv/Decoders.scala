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

package com.google.cloud.imf.bqcsv

import java.nio.charset.StandardCharsets.UTF_8
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}

import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, DateColumnVector, Decimal64ColumnVector, DoubleColumnVector, LongColumnVector, TimestampColumnVector}
import org.apache.orc.TypeDescription


object Decoders {
  case class StringDecoder(length: Int = -1) extends Decoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      val dest = bcv.getValPreallocatedBytes
      val destPos = bcv.getValPreallocatedStart
      val bytes = s.getBytes(UTF_8)
      System.arraycopy(bytes, 0, dest, destPos, bytes.length)
      bcv.setValPreallocated(i, bytes.length)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(maxSize)
      cv
    }

    override def typeDescription: TypeDescription =
      if (length > 0) TypeDescription.createChar.withMaxLength(length)
      else TypeDescription.createChar
  }

  case class Int64Decoder() extends Decoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val long = s.toLong
      row.asInstanceOf[LongColumnVector].vector.update(i, long)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong
  }

  case class Float64Decoder() extends Decoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val double = s.toDouble
      row.asInstanceOf[DoubleColumnVector].vector.update(i, double)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DoubleColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDouble
  }

  case class DateDecoder(format: String = "yyyy-MM-dd") extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val dcv = row.asInstanceOf[DateColumnVector]
      val dt = LocalDate.from(fmt.parse(s)).toEpochDay
      dcv.vector.update(i, dt)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DateColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDate
  }

  case class TimestampDecoder(format: String = "yyyy-MM-dd HH:mm:ssz") extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val tcv = row.asInstanceOf[TimestampColumnVector]
      val timestamp = ZonedDateTime.from(fmt.parse(s))
      tcv.time.update(i, timestamp.toEpochSecond*1000L)
      tcv.nanos.update(i, 0)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp
  }

  case class TimestampDecoder2(format: String = "yyyy-MM-dd HH:mm:ss", offset: Int) extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    require(offset >= -18 && offset <= 18, s"invalid offset $offset")
    private final val zoneOffset = ZoneOffset.ofHours(offset)

    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val tcv = row.asInstanceOf[TimestampColumnVector]
      val timestamp = LocalDateTime.from(fmt.parse(s)).atOffset(zoneOffset)
      tcv.time.update(i, timestamp.toEpochSecond*1000L)
      tcv.nanos.update(i, 0)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp
  }

  case class DecimalDecoder(precision: Int, scale: Int) extends Decoder {
    require(precision >= 0 && precision < 38, s"invalid precision $precision")
    require(scale >= 0 && scale < 38, s"invalid scale $scale")
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val dcv = row.asInstanceOf[Decimal64ColumnVector]
      val long = s.filter(c => c.isDigit || c == '-').toLong
      dcv.vector.update(i, long)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new Decimal64ColumnVector(maxSize, precision, scale)

    override def typeDescription: TypeDescription =
      TypeDescription.createDecimal
        .withScale(scale)
        .withPrecision(precision)
  }
}
