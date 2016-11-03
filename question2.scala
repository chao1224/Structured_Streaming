import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.commons.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import scala.concurrent.duration._
import java.io._

object StructuredStreaming {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StructuredStreaming <input_dir> <output_dir>")
      System.exit(1)
    }

    // by default is /tweets
    val input_dir = args(0)
    val input_file = input_dir + "/*.csv"
    // by default is temp_output_file
    val output_dir = args(1)
    val output_path = new Path(output_dir + "/temp")

    var maprfsCoreSitePath = new Path("core-site.xml")
    var maprfsSitePath = new Path("maprfs-site.xml")
    var conf = new Configuration()
    conf.addResource(maprfsCoreSitePath)
    conf.addResource(maprfsSitePath)
    val fileSystem = FileSystem.get(conf)
    if (!fileSystem.exists(output_path)) {
        fileSystem.mkdirs(output_path)
    }

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
            var fileWriter: FSDataOutputStream  = _
            var batch = 0

            override def open(partitionId: Long, version: Long): Boolean = {
              batch += 1
              fileWriter = fileSystem.create(output_path, false)
              true
            }
            override def process(record: Row): Unit = {
              fileWriter.write(record.getAs("B"))
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