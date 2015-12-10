package com.nxtwv.utils

import java.io.{StringWriter, BufferedReader, InputStreamReader, FileInputStream}
import java.util.zip.GZIPInputStream

import scala.io.BufferedSource


/**
 * Created by corey auger on 14/10/14.
 */

case class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
  override def hasNext() = reader.ready
  override def next() = reader.readLine()
}

object GZipFileIterator {
  def apply(file: java.io.File, encoding: String) = {
    new BufferedReaderIterator(
      new BufferedReader(
        new InputStreamReader(
          new GZIPInputStream(
            new FileInputStream(file)), encoding)))
  }
}

object GZipFileContents {
  def apply(file: java.io.File, encoding: String):BufferedSource = {
    scala.io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(file)))
  }
}

