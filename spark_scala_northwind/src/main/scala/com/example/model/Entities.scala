package com.example.model

case class Order(orderID:Int, customerID:String, employeeID:Int, shipCity:String, shipRegion:String, shipPostalCode:String)
case class OrderDetails(orderID:Int,productID:Int,unitPrice:Double,quantity:Int,discount:Double)
case class Product(productID:Int,productName:String,supplierID:Int,categoryID:Int,quantityPerUnit:String,unitPrice:Double)
case class Employee(employeeID:String,lastName:String,firstName:String,title:String,titleOfCourtesy:String,birthDate:String,address:String,city:String)
case class Customer(customerID:String,companyName:String,contactName:String,contactTitle:String,address:String,city:String)
case class Shipper(shipperID:String,companyName:String,phone:String)
case class Supplier(supplierID:String,companyName:String,contactName:String,contactTitle:String,address:String,city:String)