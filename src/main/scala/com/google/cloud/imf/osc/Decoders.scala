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

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}

import com.google.cloud.bigquery.StandardSQLTypeName
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, DateColumnVector,
  Decimal64ColumnVector, DoubleColumnVector, LongColumnVector, TimestampColumnVector}
import org.apache.orc.TypeDescription


object Decoders {
  case class StringDecoder(length: Int = -1) extends Decoder {
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val bcv = column.asInstanceOf[BytesColumnVector]
      if (s.isEmpty){
        bcv.isNull.update(i, true)
        bcv.setValPreallocated(i,0)
        if (bcv.noNulls) bcv.noNulls = false
      } else {
        val bytes = s.getBytes(UTF_8)
        bcv.ensureValPreallocated(bytes.length)
        val dst = bcv.getValPreallocatedBytes
        val dstPos = bcv.getValPreallocatedStart
        System.arraycopy(bytes, 0, dst, dstPos, bytes.length)
        bcv.setValPreallocated(i,bytes.length)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(maxSize)
      cv
    }

    override def typeDescription: TypeDescription =
      if (length > 0)
        TypeDescription.createVarchar().withMaxLength(length)
      else
        TypeDescription.createString()

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.STRING
  }

  case class Int64Decoder() extends Decoder {
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val lcv = column.asInstanceOf[LongColumnVector]
      if (s.isEmpty){
        lcv.isNull.update(i, true)
        if (lcv.noNulls) lcv.noNulls = false
      } else {
        lcv.vector.update(i, StringToNum.longValue(s.trim))
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.INT64
  }

  case class Float64Decoder() extends Decoder {
    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val dcv = column.asInstanceOf[DoubleColumnVector]
      if (s.isEmpty){
        dcv.isNull.update(i, true)
        if (dcv.noNulls) dcv.noNulls = false
      } else {
        val double = s.trim.toDouble
        dcv.vector.update(i, double)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DoubleColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDouble

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.FLOAT64
  }

  val Offset = "XXXXX"
  val DateFormat = "yyyy-MM-dd"
  val LocalFormat = "yyyy-MM-dd HH:mm:ss"
  val OffsetFormat = "yyyy-MM-dd HH:mm:ssXXXXX"
  val ZoneFormat = "yyyy-MM-dd HH:mm:ss z"
  val ZoneFormat2 = "yyyy-MM-dd HH:mm:ssz"
  val UTC: ZoneId = ZoneId.of("Etc/UTC")

  case class DateDecoder(format: String = DateFormat) extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val dcv = column.asInstanceOf[DateColumnVector]
      if (s.isEmpty){
        dcv.isNull.update(i, true)
        if (dcv.noNulls) dcv.noNulls = false
      } else {
        val dt = LocalDate.from(fmt.parse(s.trim)).toEpochDay
        dcv.vector.update(i, dt)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DateColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDate

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.DATE
  }

  case class TimestampDecoder(format: String = OffsetFormat) extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val tcv = column.asInstanceOf[TimestampColumnVector]
      if (s.isEmpty){
        tcv.isNull.update(i, true)
        if (tcv.noNulls) tcv.noNulls = false
      } else {
        val timestamp = OffsetDateTime.from(fmt.parse(s.trim)).atZoneSameInstant(UTC)
        tcv.time.update(i, Timestamp.valueOf(timestamp.toLocalDateTime).getTime)
        tcv.nanos.update(i, 0)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.TIMESTAMP
  }

  case class TimestampDecoderZoned(format: String = ZoneFormat) extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val tcv = column.asInstanceOf[TimestampColumnVector]
      if (s.isEmpty){
        tcv.isNull.update(i, true)
        if (tcv.noNulls) tcv.noNulls = false
      } else {
        val timestamp = ZonedDateTime.from(fmt.parse(s.trim)).withZoneSameInstant(UTC)
        tcv.time.update(i, Timestamp.valueOf(timestamp.toLocalDateTime).getTime)
        tcv.nanos.update(i, 0)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp

    override def bqType: StandardSQLTypeName = StandardSQLTypeName.TIMESTAMP
  }

  case class TimestampDecoder2(format: String = LocalFormat, zoneId: String) extends Decoder {
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    private final val zone = ZoneId.of(zoneId)

    override def get(s: String, column: ColumnVector, i: Int): Unit = {
      val tcv = column.asInstanceOf[TimestampColumnVector]
      if (s.isEmpty){
        tcv.isNull.update(i, true)
        if (tcv.noNulls) tcv.noNulls = false
      } else {
        val timestamp = LocalDateTime.from(fmt.parse(s.trim)).atZone(zone).withZoneSameInstant(UTC)
        tcv.time.update(i, Timestamp.valueOf(timestamp.toLocalDateTime).getTime)
        tcv.nanos.update(i, 0)
      }
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
//      println(column.getClass)
      val dcv = column.asInstanceOf[Decimal64ColumnVector]
      if (s.isEmpty){
        dcv.isNull.update(i, true)
        if (dcv.noNulls) dcv.noNulls = false
      } else {
        dcv.vector.update(i, StringToNum.decimalValue(s, scale))
      }
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
