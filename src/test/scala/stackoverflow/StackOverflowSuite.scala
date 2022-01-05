package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `Test grouped()`: Unit = {
    val postQ1 =    Posting(1, 1, Some(2), None, 1, Some("Scala"))
    val postA1Q1 =  Posting(2, 2, None, Some(1), 1, Some("Scala"))
    val postA2Q1 =  Posting(2, 3, None, Some(1), 2, Some("Scala"))
    val postQ2 =    Posting(1, 4, None, None, 1, Some("Java"))
    val postA1Q2 =  Posting(2, 5, None, Some(4), 0, Some("Java"))
    val postQ3 =    Posting(1, 6, None, None, 0, Some("Perl"))


    val seqPosting = Seq(postQ1, postA1Q1, postA2Q1, postQ2, postA1Q2, postQ3)
    val postingRdd = sc.parallelize(seqPosting)

    val serviceStackOverflow = new StackOverflow
    val resultGroupedArray = serviceStackOverflow.groupedPostings(postingRdd).collect()

    val expected = Array(
      (1, Iterable((postQ1, postA1Q1), (postQ1, postA2Q1))),
      (4, Iterable((postQ2, postA1Q2)))
    )

    resultGroupedArray.foreach(g => {
      println(g)
    })
    assert(resultGroupedArray sameElements expected.reverse)
  }


  @Test def `vector()`: Unit = {
    val postQ1 =    Posting(1, 1, Some(2), None, 1, Some("Scala"))
    val postQ2 =    Posting(1, 4, None, None, 1, Some("Java"))
    val postQ3 =    Posting(1, 6, None, None, 13, Some("Perl"))
    val postQ4 =    Posting(1, 7, None, None, 9, Some("Java"))
    val scoredRdd = sc.parallelize(Seq((postQ1, 150), (postQ2, 5), (postQ3, 15), (postQ4, 10)))

    val serviceStackOverflow = new StackOverflow
    val result = serviceStackOverflow.vectorPostings(scoredRdd).collect()
    result.foreach(r => println(r))

    val expected = Array(
      (500000, 150),
      (50000, 5),
      (450000, 15),
      (50000, 10)
    )

    assert(result sameElements expected)
  }


  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
