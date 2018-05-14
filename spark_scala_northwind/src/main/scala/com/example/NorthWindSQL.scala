package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object  NorthWindSQL extends App{
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
    .csv("data/northwind/orders.csv");
  val orderDetailsDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/order_details.csv");
  val productsDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/products.csv");
  val employeesDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/employees.csv");
  val customersDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/customers.csv");
  val shippersDf: Dataset[Row] = spSession.read.option("header", true)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .csv("data/northwind/shippers.csv");
  ordersDf.createOrReplaceTempView("Orders")
  orderDetailsDf.createOrReplaceTempView("OrderDetails")
  productsDf.createOrReplaceTempView("Products")
  employeesDf.createOrReplaceTempView("Employees")
  customersDf.createOrReplaceTempView("Customers")
  shippersDf.createOrReplaceTempView("Shippers")

  val orderFromGer="""SELECT customerID as client,shipAddress as address,shipCity as city from Orders
      WHERE shipCountry = 'Germany'
    """
  val orderFromGerResult=spSession.sql(orderFromGer)
  orderFromGerResult.show()
  val frightGroupedByCoun="""SELECT CAST(SUM(freight) AS DECIMAL(10,2)) as freight FROM Orders Group by shipCountry """
  val frightGroupedByCounResult=spSession.sql(frightGroupedByCoun)
  frightGroupedByCounResult.show()

  val ordersAbove100="""SELECT orderID ,productID,( CAST(unitPrice AS DECIMAL(10,2))*CAST(quantity AS DECIMAL(10,2))*(1-CAST(discount AS DECIMAL(10,2))) ) as price from OrderDetails
  """
  val ordersAbove100Result=spSession.sql(ordersAbove100)
  ordersAbove100Result.show()

  val threeTableJoin="""SELECT customerID as client,productName,shipAddress as address,shipCity as city from Orders o
    INNER JOIN OrderDetails od on o.orderID = od.orderID INNER JOIN Products p ON p.productID = od.productID
    WHERE shipCountry = 'Germany'
  """
  val threeTableJoinResult=spSession.sql(threeTableJoin)
  threeTableJoinResult.show()


  val mostExoensiveProduct="""
    SELECT productName,unitPrice FROM Products ORDER BY unitPrice DESC
  """
  val mostExoensiveProductResult=spSession.sql(mostExoensiveProduct)
  mostExoensiveProductResult.take(1).foreach((row)=>{
    println(row)
  })

  val productsRepeatedMoreThan5InGermany="""
    SELECT p.productID,productName,COUNT(od.orderID) as times_Ordered
    FROM Orders o
    INNER JOIN OrderDetails od on o.orderID = od.orderID INNER JOIN Products p ON p.productID = od.productID
    GROUP BY p.productID,productName,o.shipCountry
    HAVING o.shipCountry = 'Germany' AND COUNT(od.orderID)>5
    ORDER BY times_Ordered DESC
  """
  val productsRepeatedMoreThan5InGermanyResult=spSession.sql(productsRepeatedMoreThan5InGermany)
  productsRepeatedMoreThan5InGermanyResult.show()

  val empManager="""
    SELECT emp.firstName as Emp_Name,mang.firstName as Manager_Name FROM Employees emp LEFT JOIN Employees mang ON emp.reportsTo = mang.employeeID
  """
  val empManagerResult=spSession.sql(empManager)
  empManagerResult.show()

  val oldestEmployee="""
      SELECT emp.firstName ,CAST(datediff(current_date,CAST(emp.birthDate AS Date))/365 AS DECIMAL(4,0)) AS AGE FROM Employees emp
      ORDER BY AGE DESC
    """
  val oldestEmployeeResult=spSession.sql(oldestEmployee)
  oldestEmployeeResult.show()

  val shippedToMadridBySpeedy=
    """SELECT cus.contactName,emp.firstName FROM Orders o INNER JOIN Customers cus ON
       o.customerID = cus.customerID INNER JOIN Employees emp ON o.employeeID=emp.employeeID
       INNER JOIN Shippers sh ON o.shipVia=sh.shipperID WHERE o.shipCity='Madrid' AND sh.companyName='Speedy Express'
    """
  val shippedToMadridBySpeedyResult=spSession.sql(shippedToMadridBySpeedy)
  shippedToMadridBySpeedyResult.show()

}
