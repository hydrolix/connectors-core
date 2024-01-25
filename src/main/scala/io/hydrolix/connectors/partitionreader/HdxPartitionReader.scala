/*
 * Copyright (c) 2023-2024 Hydrolix Inc.
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

package io.hydrolix.connectors.partitionreader

import java.io._
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.{stream => jus}

import com.google.common.base.{Supplier, Suppliers}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.api.HdxStorageSettings
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan}

/**
 * Grab `stream` to consume data. It blocks the calling thread!
 */
abstract class HdxPartitionReader[T >: Null <: AnyRef](doneSignal: T, outputFormat: String) {
  private val log = LoggerFactory.getLogger(getClass)

  val info: HdxConnectionInfo
  val storage: HdxStorageSettings
  val scan: HdxPartitionScanPlan

  /**
   * Called in a new thread to handle stdout from the hdx_reader child process. Call `enqueue(value)` for each datum
   * read, then `enqueue(doneSignal)` when no more data will be forthcoming. Implementer should close the stream before
   * calling `enqueue(doneSignal)`.
   */
  protected def handleStdout(stdout: InputStream): Unit

  private val stdoutQueue = new ArrayBlockingQueue[T](1024)

  /**
   * Called by the `handleStdout` function to enqueue a value, or doneSignal
   */
  def enqueue(value: T): Unit = {
    stdoutQueue.put(value) // TODO it's possible this could block forever if the queue is full and the consumer is stalled
  }

  // Wait until the caller starts consuming to actually start the process
  private val process: Supplier[HdxReaderProcess] = Suppliers.memoize { () =>
    HdxReaderProcess(info, storage, scan, outputFormat, handleStdout)
  }

  @volatile private var datum: T = _

  val stream: jus.Stream[T] = jus.Stream.iterate(
    null, // Initial element: observed by hasNext as a start signal, but dropped from the resulting stream
    { _: T =>
      // Get the process if it's already running; launch it if not
      val proc = process.get()

      val got = stdoutQueue.take()
      if (got eq doneSignal) {
        proc.waitForExit()
        false
      } else {
        datum = got
        true
      }
    },
    { (_: T) =>
      if (datum == null) sys.error("No value stashed from hasNext!")
      datum
    }
  ).skip(1) // discard null seed value

  def close(): Unit = {
    val proc = process.get()
    if (!proc.finished.await(30, TimeUnit.SECONDS)) {
      log.warn("Timed out waiting for queue to be consumed")
    }

    proc.close()
  }
}
