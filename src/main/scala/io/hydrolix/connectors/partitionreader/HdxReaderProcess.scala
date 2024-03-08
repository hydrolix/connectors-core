package io.hydrolix.connectors.partitionreader

import java.io._
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.zip.GZIPInputStream
import scala.collection.mutable
import scala.sys.process.{Process, ProcessIO}
import scala.util.Using
import scala.util.Using.resource
import scala.util.control.Breaks.{break, breakable}

import com.google.common.io.ByteStreams
import com.typesafe.scalalogging.Logger

import io.hydrolix.connectors.api.{HdxOutputColumn, HdxStorageSettings}
import io.hydrolix.connectors.{Etc, HdxConnectionInfo, HdxPartitionScanPlan, HdxPushdown, JSON, RmRfThread, spawn}

object HdxReaderProcess {
  private val dontDelete = System.getenv("hdx_reader_no_delete") != null
  private val DockerPathPrefix = "/hdx-reader"
  private val HdxFs = "hdxfs"
  private val StderrFilterR = """^read hdx_partition=(.*?) rows=(.*?) values=(.*?) in (.*?)$""".r

  private val log = Logger(getClass)

  /**
   * Read lines from `stream`, calling `onLine` when a line is read, then `onDone` when EOF is reached.
   * Must be called in its own thread, because it does blocking reads!
   *
   * Closes the stream.
   */
  private def readLines(stream: InputStream, onLine: String => Unit, onDone: => Unit): Unit = {
    Using.resource(new BufferedReader(new InputStreamReader(stream, "UTF-8"))) { reader =>
      breakable {
        while (true) {
          val line = reader.readLine()
          try {
            if (line == null) {
              onDone
              break()
            } else {
              onLine(line)
            }
          } catch {
            case _: InterruptedException =>
              // If we got killed early (e.g. because of a LIMIT) let's be quiet about it
              Thread.currentThread().interrupt()
              break()
          }
        }
      }
    }
  }

  private val tmpDirs = new ConcurrentHashMap[(HdxConnectionInfo, HdxStorageSettings), (File, File, File)]()

  private def setupTmpDir(info: HdxConnectionInfo, storage: HdxStorageSettings): (File, File, File) = {
    tmpDirs.computeIfAbsent((info, storage), { _ =>
      val hdxReaderRoot = {
        Files.createTempDirectory("hdx_reader").also { path =>
          if (!dontDelete) {
            new RmRfThread(path.toFile).hook()
          }
        }.toFile
      }

      val hdxFs =
        new File(hdxReaderRoot, HdxFs)
          .also(_.mkdir())

      val turbineCmd = new File(hdxReaderRoot, "turbine_cmd.exe")
      Using.Manager { use =>
        ByteStreams.copy(
          use(getClass.getResourceAsStream("/linux-x86-64/turbine_cmd")),
          use(new FileOutputStream(turbineCmd))
        )
      }.get

      turbineCmd.setExecutable(true)

      // TODO maybe make this optional or record the success somewhere so we don't need to repeat it constantly
      spawn(turbineCmd.getAbsolutePath) match {
        case (255, "", "No command specified") => // OK
        case (exit, out, err) =>
          if (info.turbineCmdDockerName.isEmpty) {
            log.warn(s"turbine_cmd may not work on this OS, it exited with code $exit, stdout: $out, stderr: $err")
          }
      }

      log.info(s"Extracted turbine_cmd binary to ${turbineCmd.getAbsolutePath}")

      val turbineIniBefore = TurbineIni(
        storage,
        info.cloudCred1,
        info.cloudCred2,
        if (info.turbineCmdDockerName.isDefined) s"$DockerPathPrefix/$HdxFs" else hdxFs.getAbsolutePath
      )

      val turbineIniAfter = if (storage.cloud == "gcp" || storage.cloud == "gcs") {
        val gcsKeyFile = File.createTempFile("turbine_gcs_key", ".json", hdxReaderRoot)

        val turbineIni = Using.Manager { use =>
          // For gcs, cloudCred1 is a base64(gzip(gcs_service_account_key.json)) and cloudCred2 is unused
          val gcsKeyB64 = Base64.getDecoder.decode(info.cloudCred1)

          val gcsKeyBytes = ByteStreams.toByteArray(use(new GZIPInputStream(new ByteArrayInputStream(gcsKeyB64))))
          use(new FileOutputStream(gcsKeyFile)).write(gcsKeyBytes)

          val gcsKeyPath = if (info.turbineCmdDockerName.isDefined) {
            s"$DockerPathPrefix/${gcsKeyFile.getName}"
          } else {
            gcsKeyFile.getAbsolutePath
          }
          val turbineIniWithGcsCredsPath = turbineIniBefore.replace("%CREDS_FILE%", gcsKeyPath)

          turbineIniWithGcsCredsPath
        }.get

        turbineIni
      } else {
        // AWS doesn't need any further munging of turbine.ini
        turbineIniBefore
      }

      val turbineIniTmp = File.createTempFile("turbine", ".ini", hdxReaderRoot)
      resource(new FileOutputStream(turbineIniTmp)) {
        _.write(turbineIniAfter.getBytes("UTF-8"))
      }

      (hdxReaderRoot, turbineCmd, turbineIniTmp)
    })
  }

  /**
   * Construct a new HdxReaderProcess. It's pretty heavyweight; it creates multiple temp files and spawns a
   * `turbine_cmd hdx_reader` child process, so don't call this unless you're pretty sure it's likely to succeed.
   */
  def apply(info: HdxConnectionInfo,
         storage: HdxStorageSettings,
            scan: HdxPartitionScanPlan,
    outputFormat: String,
    handleStdout: InputStream => Unit)
                : HdxReaderProcess =
  {
    // This is done early so it can crash before creating temp files etc.
    val schema = scan.schema.fields.map { fld =>
      HdxOutputColumn(fld.name, scan.hdxCols.getOrElse(fld.name, sys.error(s"No HdxColumnInfo for ${fld.name}")).hdxType)
    }

    val (hdxReaderTmp, turbineCmd, turbineIniTmp) = setupTmpDir(info, storage)

    // TODO does anything need to be quoted here?
    //  Note, this relies on a bunch of changes in hdx_reader that may not have been merged to turbine/turbine-core yet,
    //  see https://hydrolix.atlassian.net/browse/HDX-3779
    val schemaStr = JSON.objectMapper.writeValueAsString(schema)

    val exprArgs = {
      val renderedPreds = scan.pushed.flatMap { predicate =>
        HdxPushdown.renderHdxFilterExpr(predicate, scan.primaryKeyField, scan.hdxCols)
      }

      if (renderedPreds.isEmpty) Nil else {
        List(
          "--expr",
          renderedPreds.mkString("[", " AND ", "]")
        )
      }
    }

    val turbineIniPath = if (info.turbineCmdDockerName.isDefined) {
      s"$DockerPathPrefix/${turbineIniTmp.getName}"
    } else {
      turbineIniTmp.getAbsolutePath
    }

    val turbineCmdArgs = List(
      "hdx_reader",
      "--config", turbineIniPath,
      "--output_format", outputFormat,
      "--output_path", "-",
      "--hdx_partition", s"${scan.partitionPath}",
      "--schema", schemaStr
    ) ++ exprArgs

    val cmdAndArgs = info.turbineCmdDockerName match {
      case Some(imageName) =>
        // docker run -v ~/dev/hydrolix/hdx-spark/src/main/resources/linux-x86-64:/hdx-spark -w /hdx-spark ubuntu:22.04 ./turbine_cmd -a hdx_reader ...
        List(
          "docker", // TODO this assumes docker is on the PATH
          "run",
          "-a", "STDOUT",
          "-a", "STDERR",
          "-v", s"${hdxReaderTmp.getAbsolutePath}:$DockerPathPrefix",
          imageName,
          s"$DockerPathPrefix/${turbineCmd.getName}"
        ) ++ turbineCmdArgs
      case None =>
        turbineCmd.getAbsolutePath +: turbineCmdArgs
    }

    val stderrLines = mutable.ListBuffer[String]()
    log.info(s"Running ${cmdAndArgs.mkString(" ")}")

    val child = Process(
      cmdAndArgs
    ).run(
      new ProcessIO(
        _.close(), // Don't care about stdin
        handleStdout,
        { stderr =>
          readLines(stderr,
            {
              case StderrFilterR(_*) => () // Ignore expected output
              case l => stderrLines += l // Capture unexpected output
            },
            () // No need to do anything special when stderr drains
          )
        }
      )
    )

    new HdxReaderProcess(child, stderrLines)
  }
}

final class HdxReaderProcess(           process: Process,
                             val    stderrLines: mutable.ListBuffer[String])
{
  import HdxReaderProcess._

  val finished = new CountDownLatch(1)

  def waitForExit(): Unit = {
    // No more records will be produced, stdout is closed, now we can wait for the sweet release of death
    if (process.isAlive()) {
      log.info("Waiting for child process to exit...")
    }
    val exit = process.exitValue()
    log.info(s"Child process exited with status $exit")
    finished.countDown()

    val err = stderrLines.mkString("\n  ", "\n  ", "\n")

    if (exit != 0) {
      sys.error(s"turbine_cmd process exited with code $exit; stderr was $err")
    } else {
      if (err.trim.nonEmpty) log.warn(s"turbine_cmd process exited with code $exit but stderr was: $err")
    }
  }

  def close(): Unit = {
    if (process.isAlive()) {
      log.info("Child process is still alive; killing")
      process.destroy()
    }
  }
}
