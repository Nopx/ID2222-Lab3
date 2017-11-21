import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.Array

object MiningDataStreams {


  def main(args: Array[String]): Unit = {
    // Retrieve the data from file and convert to a list of itemsets
    val conf = new SparkConf()
      .setAppName("Similar Items")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val filePath = "src/moreno_health/out.moreno_health_health"

    val graph = sc.textFile(filePath).cache()
        .map(s=>s.split(" "))
    val nodes = graph.map(_(0))
    val count = hyperLogLog(nodes,1)
    println(count)
  }

  def hyperLogLog(stream: RDD[String], hashAmount: Int): Int = {
    var b=1
    if(hashAmount>3)
      b = Math.log(2*(hashAmount-1)).toInt
    val hashLength =Integer.BYTES*8-b
    val countersSize = Math.pow(b,2).toInt
    var counters = Array.fill(countersSize)(0)

    stream.foreach { n =>
      val nHash = n.hashCode
      val lzs = leadingZeroes(extractHash(nHash,b))
      val index = extractIndex(nHash,b)
      if(counters(index)<lzs)
        println(index + "---"+counters(index)+"---"+lzs)
        counters(index) = lzs
    }
    val constant =1
    val inverseHarmonicCount = counters.//map(-_).
      reduce(Math.pow(2,_).toInt+Math.pow(2,_).toInt)
    //var aggregate = Math.pow(inverseHarmonicCount,-1) //SUM OF ALL (2^(-M[i])) ^-1 like in the HyperLogLog
    var aggregate = constant*Math.pow(countersSize,2)*inverseHarmonicCount
    //TODO work with this print
    counters.foreach(println)

    return aggregate.toInt
  }

  def extractIndex(num: Int, b: Int): Int = num >> Integer.BYTES*8-b

  def extractHash(num: Int, b: Int): Int = num & (Math.pow(2,Integer.BYTES*8-b).toInt-1)

  def leadingZeroes(num: Int): Int = {
    var result =0
    var number = num
    while(number!=0) {
      if ((number & 1) == 0)
        result += 1
      else
        number = 0 //artificial break statement
      number = number >>1
    }
    return result
  }

}