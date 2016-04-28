
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat

//Author : Habiba Neelesh

object CarrierMatch {

    def main(args: Array[String]) {
		val conf = new SparkConf().
            setAppName("Carrier Match").
            setMaster("local") // comment this line while running on emr.
		val sc = new SparkContext(conf)
		val file = sc.textFile(args(0))

        val dest = file.
			filter {t => val r = SanityCheck.parseCSVLine(t); SanityCheck.validRow(r)}.
			map { t=> val r = SanityCheck.parseCSVLine(t);
				var timeZone = toMin(r(40)) - toMin(r(29)) - r(50).toFloat.toInt
                var depDelay = r(31).toFloat.toInt;
                
                var CRSArrTime = toMin(r(40));
                var CRSDepTime = toMin(r(29));
                
                var arrTime = toMin(r(41));
                var depTime = toMin(r(30));
                
                var CRSElapsedTime = r(50).toFloat.toInt;
                var actualElapsedTime = r(51).toFloat.toInt;
                
                var CRSDepFlightDate = getEpochMin(r(5));
                var depFlightDate = CRSDepFlightDate + depDelay;
                
                var CRSDepEpochTime = getDepartureEpochMin(CRSDepFlightDate, CRSDepTime);
                var depEpochTime = getDepartureEpochMin(depFlightDate, depTime);
                var CRSArrEpochTime = getArrivalEpochMin(CRSDepFlightDate, CRSElapsedTime, CRSDepTime, timeZone);
                var arrEpochTime = getArrivalEpochMin(depFlightDate, actualElapsedTime, depTime, timeZone);

                ((r(6)+"\t"+r(0)+"\t"+r(20)), (CRSArrEpochTime+"\t"+arrEpochTime))
			}

		val orig = file.
			filter {t => val r = SanityCheck.parseCSVLine(t); SanityCheck.validRow(r)}.
			map { t=> val r = SanityCheck.parseCSVLine(t);
				
				var timeZone = toMin(r(40)) - toMin(r(29)) - r(50).toFloat.toInt
                var depDelay = r(31).toFloat.toInt;
                
                // CRS Time
                var CRSArrTime = toMin(r(40));
                var CRSDepTime = toMin(r(29));
                
                // Actual Time
                var arrTime = toMin(r(41));
                var depTime = toMin(r(30));
                
                // Get Elapsed Time
                var CRSElapsedTime = r(50).toFloat.toInt;
                var actualElapsedTime = r(51).toFloat.toInt;
                
                // Get Flight date
                var CRSDepFlightDate = getEpochMin(r(5));
                var depFlightDate = CRSDepFlightDate + depDelay;
                

                
                // Get Time in epoch
                var CRSDepEpochTime = getDepartureEpochMin(CRSDepFlightDate, CRSDepTime);
                var depEpochTime = getDepartureEpochMin(depFlightDate, depTime);
                var CRSArrEpochTime = getArrivalEpochMin(CRSDepFlightDate, CRSElapsedTime, CRSDepTime, timeZone);
                var arrEpochTime = getArrivalEpochMin(depFlightDate, actualElapsedTime, depTime, timeZone);

                ((r(6)+"\t"+r(0)+"\t"+r(11)), (CRSDepEpochTime+"\t"+depEpochTime))
            }

        val counts = orig.cogroup(dest)
        counts.collect()
		
		val fi=counts.map{x=>
            var airline=x._1.split("\t")(0)
            var year=x._1.split("\t")(1)
            val flist = x._2._1
            val glist = x._2._2
            var connections=0
            var missed=0
			
            for(f <- flist){
                for(g <- glist){

                    var CRSDepEpochTime = g.split("\t")(0).toLong
                    var CRSArrEpochTime = f.split("\t")(0).toLong

                    if(CRSDepEpochTime >= CRSArrEpochTime + 30 && CRSDepEpochTime <= CRSArrEpochTime + 360)
                    {
                        connections += 1
                        if(g.split("\t")(1).toLong < f.split("\t")(1).toLong +30)
                        {
                            missed += 1
                        }
                    }
                }
            }
            ((airline,year),(missed,connections))
        }

        val output=fi.reduceByKey((a,b) => (a._1+b._1,a._2+b._2))
		output.saveAsTextFile(args(1))

        // Shut down Spark, avoid errors.
        sc.stop()
    }
	def getDepartureEpochMin(flightTime: Long, depTime: Long): Long = {
        flightTime + depTime
    }

    def getArrivalEpochMin(flightTime: Long, depTime: Long, elapsedTime: Long, timezone: Long): Long = {
        flightTime + depTime + elapsedTime + timezone
    }

    def toMin(hhmm : String):Int={
        (hhmm.toInt/100)*60 + hhmm.toInt%100
    }


    def getEpochMin(date: String): Long = {
        var parsed: Date = null
        try 
        {
          parsed = new SimpleDateFormat("yyyy-MM-dd").parse(date)
          (parsed.getTime / (1000 * 60))
        } 
        catch 
        {
          case e: Exception => {
            e.printStackTrace()
            0
          }
        }
    }
}

