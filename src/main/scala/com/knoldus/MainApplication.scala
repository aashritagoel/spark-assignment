package com.knoldus
import org.apache.spark.{SparkConf, SparkContext}

object MainApplication extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Spark Practice1")
  val sc = new SparkContext(conf)
  val operations = new Operations(sc)
  val keyValuePair1 = (1, 3.6)
  val keyValuePair2 = (1, 1.1)
  val rdd_1 = sc.parallelize(Seq(keyValuePair1))
  val rdd_2 = sc.parallelize(Seq(keyValuePair2))

  println("Result after subtracting values with same keys: ")
  val result = operations.getResulantPairAfterValueSubtraction(rdd_1, rdd_2)
  result.foreach(println)

  println("Result after flattening the array: ")
  val inputArrayForFlatteningValues = Array((1,Array((3,4),(4,5))),(2,Array((4,2),(4,4),(3,9))))
  operations.flattenArray(inputArrayForFlatteningValues)
  .foreach(println)

  println("Printing the Array in a particular format: ----->")
  val inputForPrintingInGivenFormat = Array((1,List(1,2,3,4)),(2,List(1,2,3,4)),(3,List(1,2,3,4)),(4,List(1,2,3,4)))
  operations.printRddInAFormat(inputForPrintingInGivenFormat)

}
