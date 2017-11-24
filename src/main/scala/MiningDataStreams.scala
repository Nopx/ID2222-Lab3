import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.Array
import java.security.MessageDigest
import java.nio.ByteBuffer
import java.io.{File, PrintWriter}

import scala.io.Source

object MiningDataStreams {


  def main(args: Array[String]): Unit = {
    // Retrieve the data from file and convert to a list of itemsets
    /*
    val conf = new SparkConf()
      .setAppName("Similar Items")
      .setMaster("local[*]")
  val sc = new SparkContext(conf)*/

    val filePath = "src/moreno_health/out.moreno_health_health"
    //val filePath = "src/actor-collaboration/out.actor-collaboration"
    //val filePath = "src/com-amazon/out.com-amazon"


    println("Applying HyperLogLog:")
    val graph = Source.fromFile(filePath).getLines
    val count = hyperLogLog(graph,1024)
    println("Estimated Size: "+count)

    println
    println("Doing Hyperball Iterations:")
    doHyperBall(filePath,7,1024)
    println
    println("Getting Hyperball")
    println(getHyperBall(filePath,6,5))
  }

  //Read the value the Hyperball gave from the output files by subtracting the target iteration by this iteration -1
  def getHyperBall(filePath: String, iterations: Int, node: Int): Int = {
    val counter1 = readCounter(filePath+"ITER"+iterations+"-NODE"+node)
    val counter2 = readCounter(filePath+"ITER"+(iterations-1)+"-NODE"+node)
    if(counter1==null || counter2==null) return 1
    else return countCounter(counter1)-countCounter(counter2)
  }

  def doHyperBall(filePath: String, iterations: Int, hashAmount: Int): Unit={
    var b=1
    if(hashAmount>2)
      b = (Math.log(2*(hashAmount-1))/Math.log(2)).toInt

    //Size the counter should have
    val countersSize = Math.pow(2,b).toInt

    val graph = Source.fromFile(filePath).getLines
    //set currentnodes to a special value that cant exist in the graph
    var currentNode = "-1"
    graph.foreach{m =>
      val n = m.split(" ")(0)
      if(n!=currentNode) {
        var counters = Array.fill(countersSize)(-1)
        currentNode=n
        val nHash = util.hashing.MurmurHash3.stringHash(n)//ByteBuffer.wrap(sha256.digest(n.getBytes)).getInt
        //count the leading zeroes of the extracted hash
        val lzs = leadingZeroes(extractHash(nHash,b))
        var index = extractIndex(nHash,b)
        if(index<0) index = -1*index+(countersSize/2)-1
        counters(index)=lzs
        writeCounter(filePath+"ITER0-NODE"+n,counters)
      }
    }
    for(i <- 1 to iterations){
      println("Iteration "+i)
      val graph = Source.fromFile(filePath).getLines
      var currentNode = "-1"
      var neighbours = List.fill(0)("")
      //iterate over all lines in the graph
      graph.foreach{m =>
        //m is the line and m is the node
        val n = m.split(" ")(0)
        //Corner Case
        if(currentNode=="-1") currentNode=n
        //do union with neighbours
        if(n!=currentNode) {
          //read current counter of X
          var counters = readCounter(filePath+"ITER"+(i-1)+"-NODE"+currentNode)

          //Do union of X and all its neighbours
          neighbours.foreach{neighbour =>
            val neighbourCounter = readCounter(filePath+"ITER"+(i-1)+"-NODE"+neighbour)
            if(neighbourCounter!=null && counters != null)
              counters = union(counters,neighbourCounter)
          }
          //write counter
          writeCounter(filePath+"ITER"+i+"-NODE"+currentNode,counters)
          //reset neighbours
          neighbours=List.fill(0)("")
          currentNode=n
        }
        //Make list of neighbours
        neighbours ::= m.split(" ")(1)
      }
    }


  }

  def hyperLogLog(stream: Iterator[String], hashAmount: Int): Double = {
    //Setting b as the log base 2 of the desired amount of hashes
    // -> if you want 32 hashes b should be 5 so the first 5 bits of the hash denote the counter index
    var b=1
    if(hashAmount>2)
      b = (Math.log(2*(hashAmount-1))/Math.log(2)).toInt

    val hashLength =Integer.BYTES*8-b
    //counterSize and hashAmount are essentially the same, except counterSize is definitely a power of 2
    val countersSize = Math.pow(2,b).toInt
    var counters = Array.fill(countersSize)(-1)


    //Getting Messagedigest once for the loop
    val sha256 = MessageDigest.getInstance("SHA-256")
    def counterRefreshing(n: String): Unit = {
      val nHash = util.hashing.MurmurHash3.stringHash(n)//ByteBuffer.wrap(sha256.digest(n.getBytes)).getInt
      //count the leading zeroes of the extracted hash
      val lzs = leadingZeroes(extractHash(nHash,b))
      var index = extractIndex(nHash,b)
      if(index<0) index = -1*index+(countersSize/2)-1
      //If the leading zeroes is bigger than the current counter at the index, then save it
      if(counters(index)<lzs)
        counters(index) = lzs
    }

    stream.foreach { m =>
      //Doing this per item, since it is a stream
      val n1 = m.split(" ")(0)
      val n2 = m.split(" ")(1)
      counterRefreshing(n1)
      counterRefreshing(n2)
    }
    //Constant taken from the HyperLogLog paper
    val constant =1.4
    //SUM OF ALL (2^(-M[i])) ^-1 like in the HyperLogLog
    counters = counters.filter(_>=0)
    val inverseHarmonicCount = Math.pow(counters.map(M => Math.pow(2,-M)).reduce(_+_),-1)
    var aggregate = constant*Math.pow(counters.size,2)*inverseHarmonicCount

    return aggregate
  }

  //get the most significant b bits
  def extractIndex(num: Int, b: Int): Int = num >> Integer.BYTES*8-b

  //turn the most significant b bits to 0
  def extractHash(num: Int, b: Int): Int = num & (Math.pow(2,Integer.BYTES*8-b).toInt-1)

  //get the leading zeroes of the bits of an Integer
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

  //Count what the counter says
  def countCounter(counter: Array[Int]): Int ={
    val constant = 1.4
    val counters = counter.filter(_>=0)
    val inverseHarmonicCount = Math.pow(counters.map(M => Math.pow(2,-M)).reduce(_+_),-1)
    var aggregate = constant*Math.pow(counters.size,2)*inverseHarmonicCount
    return aggregate.toInt
  }

  //Write the counter to a file
  def writeCounter(fileName: String, counter: Array[Int]): Unit ={
    if(counter == null) return
    val pw = new PrintWriter((new File(fileName)))
    counter.foreach(pw.println)
    pw.close
  }

  //Read a counter from a file
  def readCounter(fileName: String): Array[Int]={
    try {
      val file = Source.fromFile(fileName).getLines.toList
      return file.map(Integer.parseInt).toArray
    }
    catch{
      case fnfe: java.io.FileNotFoundException => return null
    }
  }

  def union(l1: Array[Int], l2: Array[Int]): Array[Int] = (l1 zip l2).map{case(el1,el2)=>max(el1,el2)}

  def max(num1: Int, num2: Int): Int={
    if(num1<num2)
      return num2
    return num1
  }

}