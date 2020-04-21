import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object assignment2 extends Serializable{
	
   //input filename path
   val inputFile = "sample_input.txt"
   //output folder path
   val outputFolder = "./output"



  //Unit converter(Turning different unit to "Bytes") 
  def bitstransfer(x:String): Int ={
    val bitstype = "(\\d+)([^B]?B)".r

    var sum:Int = 0
    val bitstype(numbers,unit) = x
    unit match{

      case "MB" => sum = numbers.toInt *1024 * 1024
      case "KB" => sum = numbers.toInt * 1024
      case "GB" => sum = numbers.toInt * 1024 * 1024 * 1024
      case other => sum = numbers.toInt

    }

    return sum
  }

  def main(args: Array[String]): Unit = {
    // Load input data.
    val input = sc.textFile(inputFile)


    // Split up into words.
    val words = input.filter(line => !line.isEmpty).map(line => (line.split(",").slice(0, 1).mkString, bitstransfer(line.split(",").slice(3, 4).mkString)))

    //group the value by key first for next process
    val dealedwords = words.groupByKey().mapValues(line => line.toList.sortWith(_<_)).mapValues(line => {
      //  Calculating the mean of the data size
      var sum = 0
      line.foreach(meansumline => sum += meansumline)
      val mean = sum / line.length

      // Calculating the variance of the data size
      var variance_sum : Long = 0
      line.foreach(variancesumline => variance_sum = variance_sum + (mean - variancesumline.toLong) * (mean - variancesumline.toLong))
      val variance_show : Long = variance_sum / line.length

      //Make all the required element to one string and store it into new RDD
      var transfertostring : String = ""
      transfertostring += line(0).toString+"B,"+ line(line.length-1).toString+"B,"+mean.toInt.toString+"B,"+variance_show.toString+"B"
      transfertostring
    }).sortByKey(true)
    //save to the outputfolder
    dealedwords.map(line => line._1 +"," + line._2).coalesce(1).saveAsTextFile(outputFolder)


  }
}