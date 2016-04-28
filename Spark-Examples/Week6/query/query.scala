
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//Author : Habiba Neelesh

object Query {

    def main(args: Array[String]) {
		val conf = new SparkConf().
            setAppName("Query").
            setMaster("local")
		val sc = new SparkContext(conf)
		val data = sc.objectFile[(String,String)]("../indexer/out/part-00000").
			filter {r=> r._1 == args(0) }

		data.saveAsTextFile("out")
        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

