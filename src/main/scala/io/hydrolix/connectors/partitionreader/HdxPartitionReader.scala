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
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import java.{util => ju}

import com.typesafe.scalalogging.Logger

import io.hydrolix.connectors.api.HdxStorageSettings
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan}

/**
 * Grab `iterator` to consume data. It blocks the calling thread!
 */
abstract class HdxPartitionReader[T >: Null <: AnyRef](val doneSignal: T, outputFormat: String) {
  private val log = Logger(getClass)

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
  private lazy val process = HdxReaderProcess(info, storage, scan, outputFormat, handleStdout)

  val iterator: ju.Iterator[T] = new PartitionReaderIterator[T](process, stdoutQueue, doneSignal, scan.partitionPath)

  def close(): Unit = {
    if (!process.finished.await(30, TimeUnit.SECONDS)) {
      log.warn("Timed out waiting for queue to be consumed")
    }

    process.close()
  }
}

// TODO get rid of the noisy logging
// TODO get rid of the noisy logging
// TODO get rid of the noisy logging
// TODO get rid of the noisy logging
//noinspection ConvertNullInitializerToUnderscore
final class PartitionReaderIterator[T >: Null <: AnyRef](process: => HdxReaderProcess,
                                                     stdoutQueue: BlockingQueue[T],
                                                      doneSignal: T,
                                                   partitionPath: String) extends ju.Iterator[T] {
  private val logger = Logger(getClass)

  @volatile private var datum: T = null
  @volatile private var i = 0

  override def hasNext: Boolean = {
    if (datum != null) {
      logger.debug("{} Previously stashed value not consumed yet", partitionPath)
      return true
    }

    // Launch the process
    val proc = process

    // No value yet, need to load one
    logger.debug("{} Getting next value from queue...", partitionPath)
    val got = stdoutQueue.take() // TODO add a timeout here!
    if (got eq doneSignal) {
      logger.info("{} Got poison pill; exiting", partitionPath)
      proc.waitForExit()
      false
    } else {
      i += 1
      logger.debug("{} Stashing value #{}: {}", partitionPath, i, got)
      datum = got
      true
    }
  }

  override def next(): T = {
    if (datum == null) sys.error("No value stashed from hasNext!")

    logger.debug("{} Returning stashed value #{}: {}", partitionPath, i, datum)

    val ret = datum
    datum = null // consume the value
    ret
  }
}