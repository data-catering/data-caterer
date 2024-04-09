package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.core.exception.ParseFileException
import io.github.datacatering.datacaterer.core.util.FileUtil.{getDirectory, getFileContentFromFileSystem, isCloudStoragePath}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object YamlFileParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  def parseFiles[T](folderPath: String)(implicit sparkSession: SparkSession, tag: ClassTag[T]): Array[T] = {
    if (isCloudStoragePath(folderPath)) {
      val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val allFiles = fileSystem.listFiles(new Path(folderPath), true)
      val cls = tag.runtimeClass.asInstanceOf[Class[T]]

      val parsedFileArray = scala.collection.mutable.ArrayBuffer[T]()
      while (allFiles.hasNext) {
        val currentFile = allFiles.next().getPath.toString
        val fileContent = getFileContentFromFileSystem(fileSystem, currentFile)
        val parsedFile = OBJECT_MAPPER.readValue[T](fileContent, cls)
        parsedFileArray.append(parsedFile)
      }
      parsedFileArray.toArray
    } else {
      val directory = getDirectory(folderPath)
      getNestedFiles(directory).map(f => parseFile[T](f))
    }
  }

  private def getNestedFiles(folder: File): Array[File] = {
    if (!folder.isDirectory) {
      LOGGER.warn(s"Folder is not a directory, unable to list files, path=${folder.getPath}")
      Array()
    } else {
      val current = folder.listFiles().filter(_.getName.endsWith(".yaml"))
      current ++ folder.listFiles
        .filter(_.isDirectory)
        .flatMap(getNestedFiles)
    }
  }

  private def parseFile[T](file: File)(implicit tag: ClassTag[T]): T = {
    val cls = tag.runtimeClass.asInstanceOf[Class[T]]
    Try(OBJECT_MAPPER.readValue[T](file, cls)) match {
      case Failure(exception) => throw new ParseFileException(file.getAbsolutePath, cls.getName, exception)
      case Success(value) => value
    }
  }
}
