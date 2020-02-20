package com.lvt.testspark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Function1;
import scala.runtime.BoxedUnit;

public class MainTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("EdurekaApp3").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        SparkSession spark = SparkSession.builder().appName("Simple App").getOrCreate();

        StructType customizedSchema = new StructType(new StructField[]{
                new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
                new StructField("race", DataTypes.StringType, true, Metadata.empty()),
                new StructField("parentalLevelOfEducation", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lunch", DataTypes.StringType, true, Metadata.empty()),
                new StructField("testPreparationCourse", DataTypes.StringType, true, Metadata.empty()),
                new StructField("mathScore", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("readingScore", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("writingScore", DataTypes.IntegerType, true, Metadata.empty())
        });

        String pathToFile = "/usr/local/thang/StudentsPerformance.csv";

        Dataset<Row> DF = sqlContext
                .read()
                .format("com.databricks.spark.csv")
                .schema(customizedSchema)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(pathToFile);

        System.out.println("We are starting from here...!");

        DF.rdd().cache();
        DF.registerTempTable("Student");
        sqlContext.sql("select * from Student where mathScore > 75 order by mathScore desc").show();

        spark.stop();

    }
}
