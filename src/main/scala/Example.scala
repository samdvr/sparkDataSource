import org.apache.spark.sql.SparkSession

object Example extends App {
  val spark = SparkSession.builder().master("local[*]").appName("test")
    .getOrCreate()

  val pathToFile = "somePath"

  spark.read
    .format("com.samdvr.datareader")
    .option("fileReader.path", pathToFile)
    .load
    .show
}
