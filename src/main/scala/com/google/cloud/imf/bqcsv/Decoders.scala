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
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}

import com.google.cloud.bigquery.StandardSQLTypeName
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, DateColumnVector, Decimal64ColumnVector, DoubleColumnVector, LongColumnVector, TimestampColumnVector}
import org.apache.orc.TypeDescription


object Decoders {
  case class StringDecoder(length: Int = -1) extends Decoder {
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val bcv = column.asInstanceOf[BytesColumnVector]
      val bytes = s.getBytes(UTF_8)
      bcv.setRef(i,bytes,0,bytes.length)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(maxSize)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createChar.withMaxLength(length)

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.STRING
  }

  case class Int64Decoder() extends Decoder {
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val long = s.trim.toLong
      column.asInstanceOf[LongColumnVector].vector.update(i, long)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.INT64
  }

  case class Float64Decoder() extends Decoder {
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val double = s.trim.toDouble
      column.asInstanceOf[DoubleColumnVector].vector.update(i, double)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DoubleColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDouble

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.FLOAT64
  }

  case class DateDecoder(format: String = "yyyy-MM-dd") extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val dcv = column.asInstanceOf[DateColumnVector]
      val dt = LocalDate.from(fmt.parse(s.trim)).toEpochDay
      dcv.vector.update(i, dt)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DateColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDate

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.DATE
  }

  case class TimestampDecoder(format: String = "yyyy-MM-dd HH:mm:ssz") extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val tcv = column.asInstanceOf[TimestampColumnVector]
      val timestamp = ZonedDateTime.from(fmt.parse(s.trim))
      tcv.time.update(i, timestamp.toEpochSecond*1000L)
      tcv.nanos.update(i, 0)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.TIMESTAMP
  }

  case class TimestampDecoder2(format: String = "yyyy-MM-dd HH:mm:ss", zoneId: String) extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    private final val zone = ZoneId.of(zoneId)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val tcv = column.asInstanceOf[TimestampColumnVector]
      val timestamp = LocalDateTime.from(fmt.parse(s.trim)).atZone(zone)
      tcv.time.update(i, timestamp.toEpochSecond*1000L)
      tcv.nanos.update(i, 0)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.TIMESTAMP
  }

  case class DecimalDecoder(precision: Int, scale: Int) extends Decoder {
    require(precision >= 0 && precision < 38, s"invalid precision $precision")
    require(scale >= 0 && scale < 38, s"invalid scale $scale")
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val dcv = column.asInstanceOf[Decimal64ColumnVector]
      val long = s.trim.filter(c => c.isDigit || c == '-').toLong
      dcv.vector.update(i, long)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new Decimal64ColumnVector(maxSize, precision, scale)

    override def typeDescription: TypeDescription =
      TypeDescription.createDecimal
        .withScale(scale)
        .withPrecision(precision)

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.NUMERIC
  }
}
