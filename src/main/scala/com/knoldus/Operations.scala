package com.knoldus

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Operations(sc: SparkContext) {

  def getResulantPairAfterValueSubtraction(firstPair: RDD[(Int, Double)], secondPair: RDD[(Int, Double)]): Array[(Int, Double)] = {
    val joinedRdd = firstPair join secondPair
    joinedRdd.map(pair => (pair._1, pair._2._1 - pair._2._2)).collect
  }

  def flattenArray(keyValuePairArray: Array[(Int, Array[(Int, Int)])]): Array[(Int, (Int, Int))] = {
    sc.parallelize(keyValuePairArray)
      .flatMapValues(value => value).collect
  }

  def printRddInAFormat(rdd: Array[(Int,List[Int])]): Unit = {
    val result = sc.parallelize(rdd)
      .flatMapValues(value => value).filter(x => x._1 == x._2)
    result.foreach(println)
  }


}
