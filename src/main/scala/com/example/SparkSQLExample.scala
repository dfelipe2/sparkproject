import org.apache.spark.sql.SparkSession

object SparkSQLExample{
  //create_ds
  case class Person(name: String, age: Long)

  def main(args: Array[String]){
    //start init_session
    val spark = SparkSession
      //import org.apache.spark.sql.SparkSession
      //SparkSession can create DataFrames [ from RDD, from Hive table, from Spark data sources ]
      //SparkSession support HiveQL queries, Hive UDFs access, Hive tables
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    // For implicit conversions like converting RDDs to DataFrames
    //end init_session

    //start create_df
    val df = spark.read.json("examples/src/main/resources/people.json")
    df.show()
    // Displays the content of the DataFrame to stdout
    //end create_df

    //start untyped_ops
    import spark.implicits._
    // This import is needed to use the $-notation
    df.printSchema()
    // Print the schema in a tree format
    df.select("name").show()
    // Select only the "name" column
    df.select($"name", $"age" + 1).show()
    // Select everybody, but increment the age by 1
    df.filter($"age" > 21).show()
    // Select people older than 21
    df.groupBy("age").count().show()
    // Count people by age
    //end untyped_ops

    //start run_sql
    df.createOrReplaceTempView("people")
    // Register the DataFrame as a SQL temporary view
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    //end run_sq

    //start global_temp_view
    df.createGlobalTempView("people")
    // Register the DataFrame as a global temporary view
    spark.sql("SELECT * FROM global_temp.people").show()
    // Global temporary view is tied to a system preserved database `global_temp`
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // Global temporary view is cross-session
    //end global_temp_view



    import spark.implicits._
    //encoders para los types mas comunes
    //start create_ds
    val caseClassDS = Seq(Person("Laura", 35)).toDS()
    //encoders created for case classes
    caseClassDS.show()
    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.map(_ + 1).collect()
    //returns: Array(2,3,4)









    spark.stop()
  }
}