package io.hydrolix.connectors.partitionreader

import java.io.{BufferedReader, InputStream, InputStreamReader}
import scala.util.Using
import scala.util.control.Breaks.{break, breakable}

import com.fasterxml.jackson.databind.node.ObjectNode

import io.hydrolix.connectors.api.HdxStorageSettings
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan, JSON}

final class RowPartitionReader[T <: AnyRef](         val           info: HdxConnectionInfo,
                                                     val        storage: HdxStorageSettings,
                                                     val primaryKeyName: String,
                                                     val           scan: HdxPartitionScanPlan,
                                                     val          parse: RowAdapter[T, _, _],
                                            override val     doneSignal: T)
  extends HdxPartitionReader[T]
{
  override def outputFormat = "json"

  override def handleStdout(stdout: InputStream): Unit = {
    var rowId = 0

    Using.Manager { use =>
      val reader = use(new BufferedReader(new InputStreamReader(stdout)))
      breakable {
        while (true) {
          val line = reader.readLine()
          if (line == null) {
            stdoutQueue.put(doneSignal)
            break()
          } else {
            rowId += 1
            expectedLines.incrementAndGet()
            val obj = JSON.objectMapper.readValue[ObjectNode](line)
            stdoutQueue.put(parse.row(rowId, scan.schema, obj))
          }
        }
      }
    }.get
  }
}
