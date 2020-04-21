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

import com.google.cloud.imf.osc.Decoders.{DateDecoder, DecimalDecoder, Float64Decoder, Int64Decoder, StringDecoder, TimestampDecoder, TimestampDecoder2, TimestampDecoderZoned}

import scala.util.Try

object SchemaInference {
  def isNumeric(s: String): Boolean = {
    s.nonEmpty &&
      s.forall{ c => c.isDigit || c == '-' || c == '.'} &&
      s.count(_ == '.') <= 1 &&
      s.count(_ == '-') <= 1
  }

  def isInt(s: String): Boolean = {
    s.nonEmpty &&
      s.forall{c => c.isDigit || c == '-'} &&
      (s.headOption.contains('-') || s.count(_ == '-') == 0) &&
      s.count(_ == '-') <= 1 &&
      s.filter(_.isDigit).length <= 18
  }

  def isDecimal(s: String): Boolean = {
    s.nonEmpty &&
      s.forall{c => c.isDigit || c == '-' || c == '.'} &&
      (s.headOption.contains('-') || s.count(_ == '-') == 0) &&
      s.count(_ == '.') <= 1 &&
      s.filter(_.isDigit).length <= 36
  }

  private val DatePattern = "yyyy-MM-dd"
  private val DatePattern2 = "yyyyMMdd"
  private val DatePattern3 = "yyyy/MM/dd"
  private val DatePattern4 = "MM/dd/yyyy"
  private val DateFormatter = DateTimeFormatter.ofPattern(DatePattern)
  private val DateFormatter2 = DateTimeFormatter.ofPattern(DatePattern2)
  private val DateFormatter3 = DateTimeFormatter.ofPattern(DatePattern3)
  private val DateFormatter4 = DateTimeFormatter.ofPattern(DatePattern4)
  private val TimeStampFormatter = DateTimeFormatter.ofPattern(Decoders.LocalFormat)
  private val ZonedTimeStampFormatter = DateTimeFormatter.ofPattern(Decoders.ZoneFormat2)
  private val ZonedTimeStampFormatter2 = DateTimeFormatter.ofPattern(Decoders.ZoneFormat)
  private val OffsetTimeStampFormatter = DateTimeFormatter.ofPattern(Decoders.OffsetFormat)

  private val formatters = Seq(
    (DateFormatter,DatePattern),
    (DateFormatter2,DatePattern2),
    (DateFormatter3,DatePattern3),
    (DateFormatter4,DatePattern4)
  )

  def isTimestamp(s: String): Boolean =
    Try(TimeStampFormatter.parse(s)).isSuccess

  def isZonedTimestamp(s: String): Boolean =
    Try(ZonedTimeStampFormatter.parse(s)).isSuccess

  def isZonedTimestamp2(s: String): Boolean =
    Try(ZonedTimeStampFormatter2.parse(s)).isSuccess

  def isOffsetTimestamp(s: String): Boolean =
    Try(OffsetTimeStampFormatter.parse(s)).isSuccess

  def getDateFormat(s: String): Option[(DateTimeFormatter,String)] = {
    formatters.find{x =>
      Try(x._1.parse(s)).isSuccess
    }
  }

  def dateFormat(data: Array[String]): Option[String] = {
    data.headOption.flatMap(getDateFormat)
      .flatMap{fmt =>
        if (data.forall(s => Try(fmt._1.parse(s)).isSuccess)){
          Option(fmt._2)
        } else None
      }
  }

  def getScale(data: Array[String]): Option[Int] = {
    val scales = data.filter(_.contains('.'))
      .flatMap(_.split('.').lastOption)
      .map(_.length)
      .distinct
    if (scales.length == 1) Option(scales.head)
    else None
  }

  def getMaxLength(data: Array[String]): Int = {
    data.map(_.length).max
  }

  def inferType(sample: Array[String], zoneId: String): Decoder = {
    val col = sample.map(_.trim)
    val maxLength = getMaxLength(col)
    if (col.forall(isNumeric)){
      if (col.forall(isInt)){
        Int64Decoder()
      } else if (col.forall(_.length <= 18)) {
        val scale = getScale(col)
        if (scale.isDefined && maxLength <= 18)
          DecimalDecoder(maxLength,scale.get)
        else Float64Decoder()
      } else StringDecoder(maxLength)
    } else if (col.forall(isZonedTimestamp)){
      TimestampDecoderZoned(Decoders.ZoneFormat)
    } else if (col.forall(isZonedTimestamp2)){
      TimestampDecoderZoned(Decoders.ZoneFormat)
    } else if (col.forall(isOffsetTimestamp)){
      TimestampDecoder(Decoders.OffsetFormat)
    } else if (col.forall(isTimestamp)) {
      TimestampDecoder2(Decoders.LocalFormat, zoneId)
    } else {
      val fmt = dateFormat(col)
      if (fmt.isDefined){
        DateDecoder(fmt.get)
      } else StringDecoder(maxLength)
    }
  }
}
