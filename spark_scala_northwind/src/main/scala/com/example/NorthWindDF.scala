import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object NorthWindDF extends App {
  System.setProperty("hadoop.home.dir", "C:\\Users\\I320209\\Documents\\WinUtils")
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  val appName = "NorthWindDF";
  val sparkMaster = "local[2]";

  val conf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMaster);
  val spSession = SparkSession.builder
    .master(sparkMaster)
    .appName(appName)
    .config(conf)
    .getOrCreate()
  val ordersDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/orders.csv").cache();
  val orderDetailsDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/order_details.csv").cache();
  val productsDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/products.csv").cache();
//  ordersDf.show()
//  orderDetailsDf.show()
//  productsDf.show()

  ordersDf.select(col("customerID") as "Client", col("orderDate"), col("shipAddress") as "address", col("shipCity") as "city")
    .filter(col("shipCountry").equalTo("Germany"))
    .show()
  //sum of all the bulk group by country
  ordersDf.groupBy(col("shipCountry")).agg(sum(col("freight"))).show()
  //find all productId and orderId which have placed order of value 100 or more
  orderDetailsDf.select(col("orderID").cast(IntegerType), col("unitPrice").cast(DoubleType), col("discount").cast(DoubleType)).show()
  orderDetailsDf.select(col("orderID"), col("productID"), col("unitPrice") * col("quantity") * (lit(1.00).cast(DoubleType) - col("discount").cast(DoubleType)) as "price paid")
    .filter(col("price paid").>(100))
    .show()
  //get all the products shipped to 'Germany'
  orderDetailsDf.join(productsDf, "productID").join(ordersDf, "orderID")
    .select(col("customerID") as "Client", col("productName"), col("shipAddress") as "address", col("shipCity") as "city")
    .filter(col("shipCountry").equalTo("Germany"))
    .show()

  //most expensive product
  productsDf.agg(max(col("unitPrice")) as "max_price").show()
  productsDf.sort(col("unitPrice").desc).take(1).foreach((row)=>{
    println(row)
  })

  /*
  * Find all orders in 1997
  * */
  ordersDf.select(col("customerID") as "Client", col("orderDate"), col("shipAddress") as "address", col("shipCity") as "city")
    .filter(col("orderDate").cast(DateType).between("1997-01-01 00:00:00.000","1997-12-31 00:00:00.000"))
    .show()
}
