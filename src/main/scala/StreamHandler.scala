import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object StreamHandler {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Stream Handler")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val inputOrderDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "orders")
            .load()

        // FOR orders
        /*
            {"id":null,"customerId":1,"orderTime":"2022-12-27","shippingCost":60000,
            "subtotal":99280000,"total":99340000,"deliverDays":6,
            "deliverDate":"2023-01-02","paymentMethod":"COD",
            "shippingAddress":"Receiver: Trung Dang. Address: BCON Suoi Tien, Di An, Binh Duong. Phone Number: 0359904878"}
        */

        val orderSchema = new StructType()
                .add("id", StringType)
                .add("customerId", LongType)
                .add("orderTime", DateType)
                .add("shippingCost", FloatType)
                .add("subtotal", FloatType)
                .add("total", FloatType)
                .add("deliverDays", IntegerType)
                .add("deliverDate", DateType)
                .add("paymentMethod", StringType)
                .add("shippingAddress", StringType)

        val rawOrderDF = inputOrderDF.selectExpr("CAST(value AS STRING)")
    
        // col("value") => data from kafka
        val ordersDF = rawOrderDF.select(from_json(col("value"), orderSchema).as("data"))
                            .select("data.*")

        val orderSummary = ordersDF.withColumnRenamed("customerId", "customer_id")
            .withColumnRenamed("orderTime", "order_time")
            .withColumnRenamed("shippingCost", "shipping_cost")
            .withColumnRenamed("deliverDays", "deliver_days")
            .withColumnRenamed("deliverDate", "deliver_date")
            .withColumnRenamed("paymentMethod", "payment_method")
            .withColumnRenamed("shippingAddress", "shipping_address")

        val orderQuery = orderSummary
            .writeStream
            .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
                println(s"Order Writing to MySQL $batchID")
                batchDF.write
                    .mode("append")
                    .format("jdbc")
                    .option("driver","com.mysql.cj.jdbc.Driver")
                    .option("url", "jdbc:mysql://20.239.84.62:3306/ecommerce")
                    .option("dbtable", "orders")
                    .option("user", "admin")
                    .option("password", "pAssWoRd@2022haCkerLord")
                    .save()
            }
            .outputMode("update")
            .start()

        val inputOrderDetailDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "order-detail")
            .load()
        // FOR order-detail
        /*
            {"quantity":2,"shippingCost":60000,"unitPrice":30390000,"subtotal":60780000,"productId":2,"orderId":null}
        */

        val orderDeDetailSchema = new StructType()
                .add("quantity", IntegerType)
                .add("shippingCost", FloatType)
                .add("unitPrice", FloatType)
                .add("subtotal", FloatType)
                .add("productId", LongType)
                .add("orderId", StringType)

        val rawOrderDetailDF = inputOrderDetailDF.selectExpr("CAST(value AS STRING)")

        val orderDetailDF = rawOrderDetailDF.select(from_json(col("value"), orderDeDetailSchema).as("data"))
                        .select("data.*")

        val orderDetailSummary = orderDetailDF.withColumnRenamed("productId", "product_id")
            .withColumnRenamed("orderId", "order_id")
            .withColumnRenamed("shippingCost", "shipping_cost")
            .withColumnRenamed("unitPrice", "unit_price")

        val orderDetailQuery = orderDetailSummary
            .writeStream
            .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
                println(s"OrderDetail Writing to MySQL $batchID")
                batchDF.write
                    .mode("append")
                    .format("jdbc")
                    .option("driver","com.mysql.cj.jdbc.Driver")
                    .option("url", "jdbc:mysql://20.239.84.62:3306/ecommerce")
                    .option("dbtable", "order_detail")
                    .option("user", "admin")
                    .option("password", "pAssWoRd@2022haCkerLord")
                    .save()
            }
            .outputMode("update")
            .start()
        
        orderQuery.awaitTermination()
        orderDetailQuery.awaitTermination()
    }
}