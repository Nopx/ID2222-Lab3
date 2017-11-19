import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object FrequentItemsets {


  def main(args: Array[String]): Unit = {
    // Retrieve the data from file and convert to a list of itemsets
    val conf = new SparkConf()
      .setAppName("Similar Items")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val filePath = "src/T10I4D100K.tinytiny.txt"

    //Minimal support
    val s = 0.005
    //Minimal confidence
    val c = 0.1

    val document = sc.textFile(filePath).cache()
      .map(line => line.split(" ").map(item=>item.toInt).toSet)
      .collect()
      .toList

    // Find all the frequent itemsets with support at least s
    val frequentItemsets = calculateFrequentItemsets(document, s)
    println("Frequent itemsets with support >= %.4f".format(s))
    frequentItemsets.foreach({case (itemset, count) => println("%s occurs %s times".format(itemset.mkString(" "), count.toString) )})

    // Find all the association rules with confidence at least c
    val associationRules = calculateAssociationRules(frequentItemsets, c)
    println("Assocation rules with support >= %.4f and confidence >= %.4f".format(s, c))
    associationRules.foreach({ case (left, right) => println("%s => %s".format(left.mkString(" "), right.mkString(" "))) })
  }

  def calculateFrequentItemsets(transactions: List[Set[Int]], minSup: Double): Map[Set[Int],Int] = {
    val items = transactions.reduce(_ ++ _) // all items
    val s =  minSup * transactions.size

    //Calculate frequent singletons
    val allUniqueItems= items.map(x=>Set(x)) //c1
    val singletons = transactions.flatMap(set => set.subsets(1))
    // map where each candidate starts with count 0
    val counts = mutable.Map(singletons.map((_, 0)): _*)
    // add 1 to value of candidate if it is present in a line of the document
    transactions.foreach(set => singletons.foreach(candidate => if (candidate.subsetOf(set)) counts(candidate) += 1))
    // Only keep candidates with count at least s
    val frequentSingletons = counts.filter({ case (itemset, count) => count >= s })

    def apriori(lastSets: Set[Set[Int]], k: Int, accSets: Map[Set[Int],Int]): Map[Set[Int],Int] = {
      //Generate candidates with items that are frequent, but not with items that are present in the subsets
      val candidates = lastSets
        .flatMap(set => (items -- set).map(item => set + item))
        .filter(set => set.subsets(k - 1).forall(lastSets.contains))

      // Count occurrences of the candidate itemsets
      // Map where each candidate starts with count 0
      val counts = mutable.Map(candidates.map((_, 0)).toSeq: _*)
      // add 1 to value of candidate if it is present in a line of the document
      transactions.foreach(set => candidates.foreach(candidate => if (candidate.subsetOf(set)) counts(candidate) += 1))
      // Only keep candidates with count at least s
      val newSets = counts.filter({ case (itemset, count) => count >= s })

      // Recurse only if there are newly mined itemsets, else return accumulated itemsets
      if (newSets.nonEmpty) apriori(newSets.keys.toSet, k + 1, accSets ++ newSets) else accSets
    }

    apriori(frequentSingletons.keys.toSet, 2, frequentSingletons.toMap)
  }

  def calculateAssociationRules(frequentItemsets:Map[Set[Int], Int], confidence:Double): Set[(Set[Int], Set[Int])]={
    val subsetsKeys = frequentItemsets.keys.toSet //initiate

    //Generate all possible association rules for the frequent itemsets
    val subsets = subsetsKeys
      .flatMap({case (set)=> set.subsets.map(subset=> (subset, set-- subset))})
      //filter out all empty sets
      .filter({case (subset, candidateRules)=> candidateRules.sum>0 && subset.sum>0})
      //group them by subset, so every subset is followed by all optional rules
      .groupBy(_._1)

    val leftsets = subsets
      .flatMap({case (left, rightSets)=>
        //determine the support of the left hand side
        val supportLeft = frequentItemsets(left)
        rightSets
          //filter if the confidence is not high enough
          .filter({case (_, right)=>
          //join the subset with the rule item
          val leftJoinRight = left ++ right
          //determine the support of the join
          val supportRightJoinLeft =  frequentItemsets(leftJoinRight)
          //calculate the confidence and compare it with the minimal confidence
          supportRightJoinLeft.toDouble / supportLeft >= confidence
        })
      })
      .toSet
    leftsets
  }

}