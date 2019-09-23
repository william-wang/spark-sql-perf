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

object BenchmarkQuery {
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
   
    // Create the specified database
    sql(s"create database $databaseName")
    
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = false)
    
    // For CBO only, gather statistics on all columns:
    //tables.analyzeTables(databaseName, analyzeColumns = true) 
    
    val tpcds = new TPCDS (sqlContext = sqlContext)
    // Set:
    val resultLocation = args(3) // place to write results
    val iterations = 1 // how many iterations of queries to run.
    //val queries = tpcds.tpcds2_4Queries // queries to run.
    val timeout = 24*60*60 // timeout, in seconds.
    
    def queries = {
      if (args(4) == "all") {
        tpcds.tpcds2_4Queries
      } else {
        val qa = args(4).split(",", 0)
        val qs = qa.toSeq  
        tpcds.tpcds2_4Queries.filter(q => {
            qs.contains(q.name)
        })
      }
    } 
    
    println(queries.size)
    sql(s"use $databaseName")
    val experiment = tpcds.runExperiment(
      queries, 
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)
     
    spark.stop()
  }
} 
