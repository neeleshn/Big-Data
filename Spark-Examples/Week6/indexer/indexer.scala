
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//Author : Neelesh

object Indexer {

    def main(args: Array[String]) {
		val conf = new SparkConf().
            setAppName("Indexer").
            setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("baseball/Appearances.csv").
			map {t=> val r = t.split(","); (r(3),(r(0)+"-"+r(1)))}

        val output = file.reduceByKey((a,b) => a+"\t"+b)

	output.saveAsObjectFile("out")
        sc.stop()
    }
}

