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

package io.hydrolix

import java.time.Instant
import java.util.UUID
import scala.sys.process.{Process, ProcessIO}

import com.google.common.io.ByteStreams

package object connectors {
  val uuid0 = UUID.fromString("00000000-0000-0000-0000-000000000000")

  case class NoSuchDatabaseException(db: String) extends RuntimeException(s"No such database: $db")
  case class NoSuchTableException(db: String, table: String) extends RuntimeException(s"No such table: $db.$table")

  implicit class StringStuff(underlying: String) {
    def noneIfEmpty: Option[String] = {
      if (underlying.isEmpty) None else Some(underlying)
    }
  }

  implicit class SeqStuff[A](underlying: Seq[A]) {
    def findSingle(f: A => Boolean, what: String = ""): Option[A] = {
      underlying.filter(f) match {
        case as: Seq[A] if as.isEmpty => None
        case as: Seq[A] if as.size == 1 => as.headOption
        case _ => sys.error(s"Multiple ${what + " "}elements found when zero or one was expected")
      }
    }

    def findExactlyOne(f: A => Boolean, what: String): A = {
      findSingle(f, what).getOrElse(sys.error(s"Expected to find exactly one $what"))
    }
  }

  def spawn(args: String*): (Int, String, String) = {
    var stdout: Array[Byte] = null
    var stderr: Array[Byte] = null
    val proc = Process(args).run(new ProcessIO(
      _.close(),
      out => stdout = ByteStreams.toByteArray(out),
      err => stderr = ByteStreams.toByteArray(err)
    ))

    (proc.exitValue(), new String(stdout).trim, new String(stderr).trim)
  }

  implicit class Etc[T](underlying: T) {
    /**
     * Like Kotlin, lets you replace this:
     *
     * {{{
     * val x = {
     *   val tmp = expr()
     *   doStuffWith(tmp)
     *   tmp
     * }
     * }}}
     * with this:
     * {{{
     *   val x = expr().also { tmp =>
     *     doStuffWith(tmp)
     *   }
     * }}}
     * or even:
     * {{{
     *   val x = expr().also(doStuffWith(_))
     * }}}
     */
    def also(f: T => Unit): T = {
      f(underlying)
      underlying
    }
  }

  def instantToMicros(inst: Instant): Long = {
    val sec = inst.getEpochSecond
    val nano = inst.getNano
    (sec * 1000000) + (nano / 1000)
  }

  def microsToInstant(micros: Long): Instant = {
    val sec = micros / 1000000
    val micro = micros % 1000000
    Instant.ofEpochSecond(sec, micro * 1000)
  }
}
