import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.concurrent.duration._
import java.io._

object test3 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StructuredStreaming <input_dir> <output_dir>")
      System.exit(1)
    }

    // by default is /tweets
    val input_dir = args(0)
    val input_file = input_dir + "/*.csv"
    // by default is temp_output_file
    val output = args(1)

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .getOrCreate()

    val schema = new StructType().add("A", "string").add("B","string").add("ts","string").add("interaction","string")

    val lines = spark.readStream
      .schema(schema)
      .csv(input_file)

    val results = lines.select("B").where("interaction='MT'")

    val query = results.writeStream
      .trigger(ProcessingTime(10.seconds))
      .foreach(new ForeachWriter[Row] {
            var fileWriter: FileWriter = _
            override def open(partitionId: Long, version: Long): Boolean = {
             // FileUtils.forceMkdir(new File(s"/output"))
              fileWriter = new FileWriter(new File(output))
              true
            }
            override def process(record: Row): Unit = {
              fileWriter.append(record.toSeq.mkString(","))
              println(record.getAs("B"))
            }
            override def close(errorOrNull: Throwable): Unit = {
              fileWriter.close()
            }
          })
      .start()

    query.awaitTermination()
  }
}