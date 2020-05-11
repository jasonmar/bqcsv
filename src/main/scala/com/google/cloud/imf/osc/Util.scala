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

import com.google.common.collect.ImmutableSet
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

object Util {
  def configureLogging(debug: Boolean): Unit = {
    val rootLogger = LogManager.getRootLogger
    if (!rootLogger.getAllAppenders.hasMoreElements) {
      rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%d{ISO8601} %-5p %c %x - %m%n")))
      LogManager.getLogger("org.apache.orc.impl").setLevel(Level.ERROR)
      LogManager.getLogger("org.apache.http").setLevel(Level.WARN)
      LogManager.getLogger("org.apache.hadoop.util").setLevel(Level.ERROR)
      LogManager.getLogger("io.grpc").setLevel(Level.WARN)
    }

    if (sys.env.get("BQCSV_ROOT_LOGGER").contains("DEBUG") || debug) {
      rootLogger.setLevel(Level.DEBUG)
    } else {
      rootLogger.setLevel(Level.INFO)
    }
  }

  final val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"
  final val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  final val Scopes = ImmutableSet.of(StorageScope, BigQueryScope)
}
