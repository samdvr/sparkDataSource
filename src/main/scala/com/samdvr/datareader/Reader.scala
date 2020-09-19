package com.samdvr.datareader


import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) =
    new FileDataSourceReader(options.getInt("totalPartitions", 5), options.get("fileReader.path").orElseThrow())
}

class FileDataSourcePartition(path: String, partitionNumber: Int, totalPartitions: Int) extends InputPartition[InternalRow]  {
  override def createPartitionReader(): InputPartitionReader[InternalRow]  = new FileDataSourcePartitionReader(path, partitionNumber, totalPartitions )
}

class FileDataSourceReader(totalPartitions: Int, path: String) extends DataSourceReader {

  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]()
    (0 until totalPartitions).foreach { partitionNumber =>
      factoryList.add(new FileDataSourcePartition(path: String,partitionNumber, totalPartitions))
    }
    factoryList
  }
}

class FileDataSourcePartitionReader(path: String, partitionNumber: Int, totalPartitions: Int) extends InputPartitionReader[InternalRow] {
  val source = Source.fromFile(path)
  val data = source.getLines().filter(x=> scala.util.hashing.MurmurHash3.stringHash(x).abs % totalPartitions == partitionNumber).toArray



  var index: Int = 0

  override def next: Boolean =  index < data.length

  override def get: InternalRow = {

    val row = InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString(data(index)) )
    index = index + 1

    row
  }

  override def close(): Unit = source.close()
}
