import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

case class UserData(id: Int, firstName: String, lastName: String)

object StreamHandler {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Stream Handler")
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val inputDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test_json")
            .load()

        inputDF.printSchema()

        val rawDF = inputDF.selectExpr("CAST(value AS STRING)")

        val schema = new StructType()
            .add("id", IntegerType)
            .add("firstName", StringType)
            .add("lastName", StringType)

        val userDF = rawDF.select(from_json(col("value"), schema).as("data"))
                            .select("data.*")

        val summary = userDF.withColumnRenamed("id", "id")
            .withColumnRenamed("firstName", "first_name")
            .withColumnRenamed("lastName", "last_name")

        val query = summary
            .writeStream
            .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
                println(s"Writing to Cassandra $batchID")
                batchDF.write
                    .cassandraFormat("user", "test_cas")
                    .mode("append")
                    .save()
            }
            .outputMode("update")
            .start()

        query.awaitTermination()
    }
}