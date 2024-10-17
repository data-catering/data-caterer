package io.github.datacatering.datacaterer.core.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets
import scala.util.matching.Regex
import scala.util.{Success, Try}

object FileUtil {

  val CLOUD_STORAGE_REGEX: Regex = "^(s3(a|n?)://|wasb(s?)://|gs://).*".r

  def isCloudStoragePath(path: String): Boolean = {
    CLOUD_STORAGE_REGEX.pattern.matcher(path).matches()
  }

  def getFile(filePath: String)(implicit sparkSession: SparkSession): File = {
    val (directFile, classFile, classLoaderFile) = getDirectAndClassFiles(filePath)
    (directFile.exists(), classFile.map(_.exists), classLoaderFile.map(_.exists)) match {
      case (true, _, _) => directFile
      case (_, Success(true), _) => classFile.get
      case (_, _, Success(true)) => classLoaderFile.get
      case _ => throw new FileNotFoundException(s"Failed for find file, path=$filePath")
    }
  }

  def getDirectory(folderPath: String): File = {
    val (directFile, classFile, classLoaderFile) = getDirectAndClassFiles(folderPath)
    (directFile.isDirectory, classFile.map(_.isDirectory), classLoaderFile.map(_.isDirectory)) match {
      case (true, _, _) => directFile
      case (_, Success(true), _) => classFile.get
      case (_, _, Success(true)) => classLoaderFile.get
      case _ => throw new FileNotFoundException(s"Failed for find directory, path=$folderPath")
    }
  }

  def writeStringToFile(fileSystem: FileSystem, filePath: String, fileContent: String): Unit = {
    val fsOutput = fileSystem.create(new Path(filePath))
    fsOutput.writeBytes(fileContent)
    fsOutput.flush()
    fsOutput.close()
  }

  def getFileContentFromFileSystem(fileSystem: FileSystem, filePath: String): String = {
    val fileContentBytes = fileSystem.open(new Path(filePath)).readAllBytes()
    new String(fileContentBytes, StandardCharsets.UTF_8)
  }

  private def getDirectAndClassFiles(filePath: String): (File, Try[File], Try[File]) = {
    val directFile = new File(filePath)
    val classFile = Try(new File(getClass.getResource(filePath).getPath))
    val classLoaderFile = Try(new File(getClass.getClassLoader.getResource(filePath).getPath))
    (directFile, classFile, classLoaderFile)
  }

}
