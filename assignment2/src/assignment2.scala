import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import scala.util.matching.Regex

/*
    COMP9313-19T2 Assignment 2, z5146092

    NOTE: Must "extends Serializable" as serializable object used
    NOTE: Must provide (or modify to) a correct inputPath AND an empty inputPath
    NOTE: Must give spark configuration in spark-shell, name as 'sc'
    NOTE: Should run by
        :load assignment2.scala
        assignment2.main(Array())
 */


object assignment2 extends Serializable {

    val inputFilePath = "input/"        // Please modify this as input file path
    val outputDirPath = "output/"       // Please modify this as output file path

    val dataRegex: Regex = """(\d+)([A-Z]+)""".r    // Regex to separate amount and unit

    // Function to convert all data amount in different units to Bytes
    def dataUnitConversion (dataStr: String): Int = {
        val dataRegex(dataAmount, dataUnit) = dataStr
        val dataAmountInt: Int = dataAmount.toInt

        // Match data unit cases
        dataUnit match {
            case "KB" => dataAmountInt * 1024
            case "MB" => dataAmountInt * 1024 * 1024
            case "GB" => dataAmountInt * 1024 * 1024 * 1024
            case "TB" => dataAmountInt * 1024 * 1024 * 1024 * 1024
            case "PB" => dataAmountInt * 1024 * 1024 * 1024 * 1024 * 1024
            case "B" => dataAmountInt
            case _ => 0
        }
    }


    def main (args: Array[String]): Unit ={

        // Read from input
        val input: RDD[String] = sc.textFile(inputFilePath)

        // Filter out empty lines and split them by comma
        val dataText: RDD[Array[String]] = input
            .filter(! _.isEmpty)
            .map(dataLine => dataLine.split(","))

        // Take the first (url) and last (data amount) elements
        val dataMap: RDD[(String, String)] = dataText
            .map(dataLine => (dataLine.head, dataLine.last))

        // Convert data amount to Bytes, and then group by key
        val groupedMap: RDD[(String, Iterable[Int])] = dataMap
            .map(dataLine => (dataLine._1, dataUnitConversion(dataLine._2)))
            .groupByKey()

        // Generate min, max, mean, variance values
        val resultMap: RDD[(String, String)] = groupedMap
            .mapValues(line => line.toList.sortWith(_ < _))     // Sort by data amount
            .mapValues(line => {

                // Calculate data mean
                var dataSum: Long = 0
                line.foreach(dataAmount => dataSum += dataAmount)
                val dataMean: Long = dataSum / line.length

                // Calculate variance
                var varianceSum: Long = 0
                line.foreach(dataAmount => varianceSum = varianceSum +
                    (dataAmount - dataMean) * (dataAmount - dataMean))
                val variance: Long = varianceSum / line.length

                // Concatenate strings as one value
                line.head.toString.concat("B,") +
                line.last.toString.concat("B,") +
                dataMean.toString.concat("B,") +
                variance.toString.concat("B")
            })

        // Save output to text file, use coalesce to merge the results into one file
        resultMap.map(x => x._1 + "," + x._2)
            .coalesce(1)
            .saveAsTextFile(outputDirPath)
    }
}

