package com.nesto.App;

import org.apache.spark.sql.SaveMode;

public class App extends App2 {

    public static void main(String[] args) {
        // Initialize
        App app = new App();

        // Actions and Output
        System.out.println(app.df.count());

        // HDFS output
        String hdfsOutputPath = "hdfs://192.168.247.6:8020/santhosh";
        app.df.write()
             .mode(SaveMode.Overwrite)
             .option("header", "true")
             .orc(hdfsOutputPath);

        // Stop SparkSession
        app.spark.stop();
    }
}
