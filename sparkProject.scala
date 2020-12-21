/* sparkProject.scala */
import org.apache.spark.sql.SparkSession 
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat


object SparkProject {
 def main(args: Array[String]) {
 
    val spark = SparkSession.builder().master("local[*]").appName("Spark Project").getOrCreate()
  
    import spark.implicits._
  
    spark.sparkContext.setLogLevel("WARN")
  
    val borrowersSchema = StructType(Array(
                       StructField("bid", IntegerType), 
                       StructField("gender", StringType), 
                       StructField("department", StringType)
                    )) 
                    
    val loansSchema = StructType(Array(
		       StructField("bibno", IntegerType), 
		       StructField("copyno", StringType), 
		       StructField("bid", IntegerType),
		       StructField("dateKey", StringType) //it must be DateType
    			)) 
    			
    //BORROWERS.TXT
    val borrowersFile = spark.sparkContext.textFile("./BORROWERS.TXT");
    
    val header = borrowersFile.first()

    val finalBorrowerFile = borrowersFile.filter(x => x != header)
                
    val borrowersRDD = finalBorrowerFile.map { line => 
        	val bid = line.split("\\|")(0).toInt
        	val gender = line.split("\\|")(1)
        	val department = line.split("\\|")(2)
        	Row(bid, gender, department)
      }
      
    val borrowersDF = spark.createDataFrame(borrowersRDD, borrowersSchema)
    borrowersDF.createOrReplaceTempView("borrowersTable")
    
    //LOANS.TXT
    val loansFile = spark.sparkContext.textFile("./LOANS.TXT");
    
    val header = loansFile.first()

    val finalLoansFile = loansFile.filter(x => x != header)
                
    val loansRDD = finalLoansFile.map { line => 
        
        	val bibno = line.split("\\|")(0).toInt
        	val copyno = line.split("\\|")(1)
        	val bid = line.split("\\|")(2).toInt
        	val dateKey = line.split("\\|")(3)//it must be date
        	Row(bibno, copyno, bid,dateKey)
      }
      
    val loansDF = spark.createDataFrame(loansRDD, loansSchema)
    loansDF.createOrReplaceTempView("loansTable")
   
    val borrowersAggregation = spark.sql("""SELECT borrowersTable.gender,borrowersTable.department,COUNT(loansTable.bid) AS loans FROM borrowersTable,loansTable WHERE borrowersTable.bid = loansTable.bid GROUP BY borrowersTable.gender,borrowersTable.department WITH CUBE""")
    borrowersAggregation.show();
    
    borrowersAggregation.createOrReplaceTempView("cube")
    
    val cub1 = spark.sql (""" SELECT * FROM cube WHERE gender IS NOT NULL AND department IS NULL""")
    
    cub1.createOrReplaceTempView("groupByGender")

    val cub2 = spark.sql (""" SELECT * FROM cube WHERE gender IS NOT NULL AND department IS NOT NULL""")

    cub2.createOrReplaceTempView("groupByBoth")
 
    val cub3 = spark.sql (""" SELECT * FROM cube WHERE gender IS NULL AND department IS NULL""")

    cub3.createOrReplaceTempView("groupByNone")

    val cub4 = spark.sql (""" SELECT * FROM cube WHERE gender IS NULL AND department IS NOT NULL""")    
    
    cub4.createOrReplaceTempView("groupByDepartment")


    
    
    val SQLstm1 = spark.sql(""" SELECT * FROM borrowersTable """)
               
    SQLstm1.show()
    
    val SQLstm2 = spark.sql(""" SELECT * FROM loansTable """)
               
    SQLstm2.show()

 /*val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
 val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
 val logData = spark.read.textFile(logFile).cache()
 val numAs = logData.filter(line => line.contains("a")).count()
 val numBs = logData.filter(line => line.contains("b")).count()
 println(s"Lines with a: $numAs, Lines with b: $numBs")
 spark.stop()*/
 }
}
