import com.example.model._
import org.apache.spark.{SparkConf, SparkContext}

object NorthWindRDD extends App{

  System.setProperty("hadoop.home.dir", "C:\\Users\\I320209\\Documents\\WinUtils")
  val appName = "NorthWindRDD";
  val sparkMaster = "local[2]";

  val conf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMaster);
  val spContext = new SparkContext(conf);
  val ordersRDDWithHeader = spContext.textFile("data/northwind/orders.csv")
  val ordersRDD =ordersRDDWithHeader.filter((line:String)=> {
    if(line.contains("orderID")){
      false
    }
    else
      true
  })
  val parseOrder=(line:String)=>{
    val columns=line.split(",")
    val orderId=columns(0).trim.toInt
    val customerID = columns(1).trim
    val employeeID = columns(2).trim.toInt
    val shipCity = columns(10)
    val shipRegion =columns(11)
    val shipPostalCode = columns(12).trim
    Order(orderId,customerID,employeeID,shipCity,shipRegion,shipPostalCode)
  }:Order
  val orders = ordersRDD.map[Order](parseOrder)
  orders.take(5).foreach((order: Order)=>{
    println(order)
  })

  val parseProduct=(line:String)=>{
    val columns=line.split(",")
    val productID=columns(0).trim.toInt
    val productName = columns(1).trim
    val supplierID = columns(2).trim.toInt
    val categoryID = columns(3).trim.toInt
    val quantityPerUnit =columns(4)
    val unitPrice = columns(5).trim.toDouble
    Product(productID,productName,supplierID,categoryID,quantityPerUnit,unitPrice)
  }:Product

  val productsRDDWithHeader = spContext.textFile("data/northwind/products.csv")
  val productsRDD =productsRDDWithHeader.filter((line:String)=> {
    if(line.contains("productID")){
      false
    }
    else
      true
  })
  val products = productsRDD.map[Product](parseProduct)
  products.take(5).foreach((product: Product)=>{
    println(product)
  })

  val parseOrderDetails = (line:String)=>{
    val columns=line.split(",")
    val orderId=columns(0).trim.toInt
    val productID=columns(1).trim.toInt
    val unitPrice = columns(2).trim.toDouble
    val quantity=columns(0).trim.toInt
    val discount = columns(2).trim.toDouble
    OrderDetails(orderId,productID,unitPrice,quantity,discount)
  }:OrderDetails

  val orderDetailsRDDWithHeader = spContext.textFile("data/northwind/order_details.csv")
  val orderDetailsRDD =orderDetailsRDDWithHeader.filter((line:String)=> {
    if(line.contains("productID")){
      false
    }
    else
      true
  })
  val orderDetails = orderDetailsRDD.map[OrderDetails](parseOrderDetails)
  orderDetails.take(5).foreach((orderDetails: OrderDetails)=>{
    println(orderDetails)
  })
}
