package com.example

import org.apache.spark.{SparkConf, SparkContext}

object SparkSample1 {
  val myAccessKey = "AKIAYGYEVONJAK7ZHRVO"
  val mySecretKey = "oBc2mVt///9dIAy/3i4bovXc2ji0aDgFuDYCEEql"
  val bucket = "bucket-spark-project"
  val filepath = "path-bucket-spark-project/importPeriod.csv"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sample")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration;
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId",myAccessKey)
    hadoopConf.set("fs.s3n.awsSecretAccessKey",mySecretKey)

    val s3data = sc.textFile("s3n://" + bucket + "/" + filepath)
    val april = s3data.filter( line => line.contains("april")).count()
    val ano2020 = s3data.filter( line => line.contains("2020")).count()
    val total = s3data.count()
    println("total lines: %s".format(total))
    println("Lines with april: %s, Lines with 2020: %s".format(april, ano2020))
  }
}