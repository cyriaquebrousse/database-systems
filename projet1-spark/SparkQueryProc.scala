package main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Cyriaque Brousse
  */
object SparkQueryProc {

  /** Mapping for SQL table CUSTOMER */
  // unused fields have been removed to save memory
  case class Customer (
      custKey: Int
      // name: String,
      // address: String,
      // nationKey: Int,
      // phone: String,
      // acctBal: Double,
      // mktSegment: String,
      // customerComment: String
      )
  {
    def this(x: Array[String]) = {
      this(x(0).toInt)
    }
  }

  /** Mapping for SQL table ORDERS */
  // unused fields have been removed to save memory
  case class Order (
      // orderKey: Int,
      custKey: Int,
      // orderStatus: String,
      // totalPrice: Double,
      // orderDate: String,
      // orderPriority: String,
      // clerk: String,
      // shipPriority: Int,
      orderComment: String
      )
  {
    def this(x: Array[String]) = {
      this(x(1).toInt, x(8))
    }
  }

  /**
    * Application entry point
    */
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      sys.error("arguments : <input-path> <output-path> <task>")
    }

    // Parameters mapping
    val inputPath = args(0)
    val task = args(2).toInt
    val outputPath = args(1) + "/out" + "_" + task

    // verifying task parameter
    if (task != 1 && task != 2) {
      sys.error("task argument must be one of 1, 2")
    }

    // Spark configuration
    val conf = new SparkConf().setAppName("Project2")
    val sc = new SparkContext(conf)

    // .tbl file import
    val customers: RDD[Customer] = sc textFile (inputPath + "/customer.tbl") map (_ split "\\|") map (new Customer(_))
    val orders: RDD[Order] = sc textFile (inputPath + "/orders.tbl") map (_ split "\\|") map (new Order(_))

    // key mapping
    val customersByKey = customers keyBy (_.custKey)
    val regexp = ".*special.*requests.*"
    val ordersByKey = orders filter (o => !(o.orderComment matches regexp)) keyBy (_.custKey)

    // sum the orders per customer
    val groupedOrders = ordersByKey.aggregateByKey(0)((acc, _) => acc + 1, _ + _)

    val innerQuery = task match {
      // LEFT OUTER JOIN takes into account all customers, even those who haven't placed any order.
      // For that we make a union on the two tables and take caution when merging.
      case 1 => customersByKey
        .map(c => (c._1, 0))
        .union(groupedOrders)
        .reduceByKey(_ + _)

      // INNER JOIN considers only the customers who have made at least one order.
      // We can just ignore the customers who haven't (no need to consider table Customers).
      case 2 => groupedOrders
    }

    // perform the outer query and output as text file
    innerQuery
      .map(_.swap)
      .aggregateByKey(0)((acc, _) => acc + 1, _ + _)
      .map(t => t._1 + "|" + t._2 + "|")
      .saveAsTextFile(outputPath)

    sc.stop
  }

}
