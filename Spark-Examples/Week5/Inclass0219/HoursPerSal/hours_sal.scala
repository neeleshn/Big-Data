
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap

object HomeRunsPerSalary {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Homeruns per Salary").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val bat = sc.textFile("baseball/Batting.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy (x => { x(0) + " " + x(1) })

        val sal = sc.textFile("baseball/Salaries.csv").
            map { _.split(",") }.
            filter { _(0) != "yearID" }.
            keyBy (x => { x(3) + " " + x(0) })

        // Join
        val data = bat.join(sal)

        // Output
        val text = data.map(x => { 
            val (k, v) = x;
            val (p, h) = v;
            if(p.length>=11 && h(4).toFloat>0){
                    val player = " "+ p(0) + " : " + h(0)
                    val hours = p(11).toFloat/h(4).toFloat
                    hours + "," + player
            } else { "0.0,0.0" }
        })

        val textAfterSort = text.collect().sortWith(_.split(",")(0).toFloat > _.split(",")(0).toFloat).take(5)
        val parallelOutput = sc.parallelize(textAfterSort)
       
        parallelOutput.saveAsTextFile("output")
        sc.stop()
    }
}

