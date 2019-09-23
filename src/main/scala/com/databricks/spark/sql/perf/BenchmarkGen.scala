/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession

object BenchmarkGen {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: pefTest <file>")
      System.exit(1)
    }
    
    // Set:
    val rootDir = args(1) // root directory of location to create data in.
    val databaseName = "test" // name of database to create.
    val scaleFactor = args(2) // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    
    val appName = args(0)
    val spark = SparkSession.builder.appName(s"$appName").getOrCreate()
    val sqlContext = spark.sqlContext
    val sql = spark.sql _
    
    // Run:
    val tables = new TPCDSTables(sqlContext,
        dsdgenDir = "/opt/tpcds-kit/tools", // location of dsdgen
        scaleFactor = scaleFactor,
        useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
        useStringForDate = false) // true to replace DateType with StringType
   
    tables.genData(
        location = rootDir,
        format = format,
        overwrite = true, // overwrite the data that is already there
        partitionTables = true, // create the partitioned fact tables
           clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
        filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
        tableFilter = "", // "" means generate all tables
        numPartitions = 100) // how many dsdgen partitions to run - number of input tasks.
     
    spark.stop()
  }
} 
