import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ASSIGNMENT_18_1 extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("example")
    .config("spark.sql.warehouse.dir","C://ACADGILD")
    .getOrCreate()

  // A CSV dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val dataset_1 = spark.sqlContext.read.csv("C:/ACADGILD/Big Data/SESSION_18/Big_Data_S18_A1/S18_Dataset_Holidays.txt")
                       .toDF("id", "source", "destination", "transport_mode", "distance", "year" )

  // Register this DataFrame as a table.
  dataset_1.createOrReplaceTempView("holidays")


  // SQL statements can be run by using the sql methods provided by sqlContext.
  val in1 = spark.sql("select year, count(*) as Number_of_passengers from holidays group by year")
  println("The distribution of the total number of air-travelers per year : ")
  in1.show()


  println("The total air distance covered by each user per year : ")
  val in2 = spark.sql("Select id, year, sum(cast(distance as INTEGER)) as total_distance " +
                          "from holidays group by id,year ORDER BY id,year")
    .collect().foreach(println)

  println("User has travelled the largest distance till date : ")
  val in3 = spark.sql("Select id, sum(cast(distance as INTEGER)) as total_distance " +
    "from holidays group by id")
    .toDF()

  // Register this DataFrame as a table.
  in3.createOrReplaceTempView("dist_sum")
  val in3_1 = spark.sql("Select * from dist_sum where total_distance = (select max(total_distance) from dist_sum)").show()

  println("The most preferred destination for all users : ")
  val in4 = spark.sql("Select destination, count(*) as count " +
    "from holidays group by destination")
    .toDF()
  
  in4.createOrReplaceTempView("dest_count")
  val in4_1 = spark.sql("Select * from dest_count where count = (select max(count) from dest_count)").show()

}
