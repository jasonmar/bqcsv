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

package com.google.cloud.imf.osc.bqexport

import java.nio.ByteBuffer
import java.time.LocalDate

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.jdk.CollectionConverters._

case class AvroField(field: Schema.Field) {
  val isRequired: Boolean = field.schema().getType != Schema.Type.UNION

  val typeSchema: Schema =
    if (isRequired)
      field.schema()
    else field.schema().getTypes.asScala
      .filterNot(_.getType == Schema.Type.NULL)
      .toArray.toIndexedSeq.headOption
      .getOrElse(Schema.create(Schema.Type.NULL))

  // buffer for BigInteger values representing Decimal field
  @transient private val decimalBuf = new Array[Byte](16)

  private val pos: Int = field.pos()
  def read(row: GenericRecord, sb: StringBuilder): Unit = {
    val v = row.get(pos)
    if (v != null){
      if (isDecimal) {
        val x = AvroUtil.readDecimal(v.asInstanceOf[ByteBuffer], decimalBuf, scale)
        sb.append(x.stripTrailingZeros().toPlainString)
      } else if (isTimestamp) {
        val x = new java.sql.Timestamp(v.asInstanceOf[Long] / 1000L)
        sb.append(AvroUtil.printTimestamp(x))
      } else if (isDate) {
        val x = LocalDate.ofEpochDay(v.asInstanceOf[Int])
        sb.append(AvroUtil.printDate(x))
      } else if (isString) {
        val x = v.asInstanceOf[org.apache.avro.util.Utf8].toString
        AvroUtil.appendQuotedString(',', x, sb)
      } else if (isLong) {
        val x = v.asInstanceOf[Long]
        sb.append(x)
      } else if (isDouble) {
        val x = v.asInstanceOf[Double]
        sb.append(x)
      } else if (isFloat) {
        val x = v.asInstanceOf[Float]
        sb.append(x)
      } else if (isBoolean) {
        val x = v.asInstanceOf[Boolean]
        sb.append(x)
      }else {
        val msg = s"unhandled type ${field.schema()} ${v.getClass.getCanonicalName}"
        throw new RuntimeException(msg)
      }
    } else {
      if (isRequired) {
        val msg = s"missing required field ${field.name()}"
        throw new RuntimeException(msg)
      }
    }
  }

  val isDecimal: Boolean = {
    val logicalType = typeSchema.getJsonProp("logicalType")
    if (logicalType != null){
      "decimal" == logicalType.getTextValue &&
        typeSchema.getType == Schema.Type.BYTES
    } else false
  }

  val isTimestamp: Boolean = {
    val logicalType = typeSchema.getJsonProp("logicalType")
    if (logicalType != null){
      "timestamp-micros" == logicalType.getTextValue &&
        typeSchema.getType == Schema.Type.LONG
    } else false
  }

  val isDate: Boolean = {
    val logicalType = typeSchema.getJsonProp("logicalType")
    if (logicalType != null){
      "date" == logicalType.getTextValue &&
        typeSchema.getType == Schema.Type.INT
    } else false
  }

  val isString: Boolean = typeSchema.getType == Schema.Type.STRING

  val isLong: Boolean =
    typeSchema.getType == Schema.Type.LONG &&
      null == typeSchema.getJsonProp("logicalType")

  val isFloat: Boolean =
    typeSchema.getType == Schema.Type.FLOAT &&
      null == typeSchema.getJsonProp("logicalType")

  val isDouble: Boolean =
    typeSchema.getType == Schema.Type.DOUBLE &&
      null == typeSchema.getJsonProp("logicalType")

  val isBoolean: Boolean =
    typeSchema.getType == Schema.Type.BOOLEAN &&
      null == typeSchema.getJsonProp("logicalType")

  val scale: Int = if (isDecimal) typeSchema.getJsonProp("scale").getIntValue else -1
}
