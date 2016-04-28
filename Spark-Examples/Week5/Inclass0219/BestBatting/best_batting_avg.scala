
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap

object BestBattingAverage {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Best Batting Average").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val batting = sc.textFile("baseball/Batting.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy (x => { x(0) + " " + x(1) })

        val pitching = sc.textFile("baseball/Pitching.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy (x => { x(0) + " " + x(1) })

        val master = sc.textFile("baseball/Master.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy { _(0) }.
            map(x => {val (k, v) = x; k -> v(15)})
    
        val joinedData = batting.join(pitching)
        val maptext = joinedData.map(x => { 
            val (k, v) = x;
            val (p, h) = v;
            if(h.length>=13 && p.length>=7 && p(6).toFloat>0){ //
                    val playerIDwithYear = h(0) + "," + h(1)
                    val hitsPerAtBats = h(13).toFloat/p(6).toFloat
                    hitsPerAtBats + "," + playerIDwithYear
            } else { "0.0,0.0" }
        })

        val mapwithPlayerID = maptext.map(x => {
            val playerID = x.split(",")(1)
            playerID -> x
        })

        val newData = mapwithPlayerID.join(master).map(_._2)

        val textSorted = newData.collect().sortWith(_._1.split(",")(0).toFloat > _._1.split(",")(0).toFloat).take(5)
        val finalOutputData = sc.parallelize(textSorted)
       
        finalOutputData.saveAsTextFile("Output")
        sc.stop()
    }
}