import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import java.util.UUID

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.spark.connector._

case class DeviceData(device: String, temp: Double, humd: Double, pres: Double)

object StreamHandler {
    def main(args: Array[String]) {

        val spark = SparkSession
            .builder
            .appName("Stream Handler")
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
    
        import spark.implicits._

        val inputDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test") 
            .load()

        val rawDF = inputDF.selectExpr("CAST(value as string)").as[String]

        val expandedDF = rawDF.map(row => row.split(","))
            .map(row => DeviceData(
                row(1),
                row(2).toDouble,
                row(3).toDouble,
                row(4).toDouble
            ))
            
        val summaryDF = expandedDF
            .groupBy("device")
            .agg(avg("temp").as("temp"),
                 avg("humd").as("humd"),
                 avg("pres").as("pres"))
            .withColumn("uuid", lit(UUID.randomUUID().toString))

        val query = summaryDF
            .writeStream
            .trigger(Trigger.ProcessingTime("5 seconds"))
            //.outputMode("update")
            .foreachBatch{
                (batchDF: DataFrame, batchID: Long) =>
                    println(s"Writting to Cassandra $batchID")
                    batchDF.write
                        .cassandraFormat("weather", "stuff")
                        .mode("append")
                        .save()
            }
            //.format("console")
            //.option("truncate", false)
            .outputMode("update")
            .start()

        query.awaitTermination()
    }
}
