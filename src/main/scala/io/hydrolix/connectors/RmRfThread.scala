package io.hydrolix.connectors

import java.io.File

import com.google.common.io.{MoreFiles, RecursiveDeleteOption}

class RmRfThread(file: File) extends Thread {
  override def run(): Unit = MoreFiles.deleteRecursively(file.toPath, RecursiveDeleteOption.ALLOW_INSECURE)
  def hook(): Unit = Runtime.getRuntime.addShutdownHook(this)
}
