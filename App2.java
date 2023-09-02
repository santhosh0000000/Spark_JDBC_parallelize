package com.nesto.App;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App2 {

    String host = "DERG.Mohs.local";
    String port = "30215";
    String username = "data_user";
    String password = "T*&T^*&TUT";
    String url = "jdbc:sap://" + host + ":" + port;
    String schema = "SAPAGUL";
    String tableName = "DEMOSAP";

    SparkSession spark;
    Dataset<Row> df;

    public App2() {

        // Spark Configuration
        spark = SparkSession
                .builder()
                .appName("SparkConnector")
                .master("spark://spark01.geepas.local:7077")
                .config("spark.executor.memory", "16g")
                .config("spark.executor.cores", "6") // Adjust based on your node's capacity
                .config("spark.executor.instances", "36") // Adjust based on your cluster size
                .config("spark.dynamicAllocation.enabled", "false")
                .config("spark.default.parallelism", "180") // 5 cores/executor * 36 executors
                .config("spark.jars", "/root/ngdbc-2.17.12.jar")
                .config("spark.cores.max", "180") // Total cores = 5 cores/executor * 36 executors
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        df = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("user", username)
                .option("password", password)
                .option("dbtable", tableName)
                .option("currentSchema", schema)
                .option("partitionColumn", "KWERT")
                .option("fetchsize","200000")
                .option("partitionColumnType", "decimal(15,2)")
                .option("numPartitions", "100") // Same as default parallelism
                .option("lowerBound", 0)
                .option("upperBound", 800000)
                .load();

        // Removed caching as it's beneficial only when df is reused
    }
}
