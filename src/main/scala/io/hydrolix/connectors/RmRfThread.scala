/*
 * Copyright (c) 2023 Hydrolix Inc.
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

package io.hydrolix.connectors

import java.io.File
import java.nio.file.FileSystemException

import com.google.common.io.{MoreFiles, RecursiveDeleteOption}
import com.typesafe.scalalogging.Logger

class RmRfThread(file: File) extends Thread {
  private val log = Logger(getClass)

  override def run(): Unit = {
    try {
      MoreFiles.deleteRecursively(file.toPath, RecursiveDeleteOption.ALLOW_INSECURE)
    } catch {
      case _: FileSystemException =>
        log.warn(s"Couldn't delete ${file.toPath}, you may want to clean it up yourself!")
      // all other exceptions can propagate
    }
  }
  def hook(): Unit = Runtime.getRuntime.addShutdownHook(this)
}
