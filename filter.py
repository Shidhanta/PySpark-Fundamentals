from pyspark import SparkContext,RDD
from pyspark.sql import SparkSession

if __name__ =="__main__":
        
        spark =  SparkSession \
            .builder \
            .appName("Creating RDD") \
            .master("local[*]") \
            .getOrCreate()

        #only display error messages to console
        spark.sparkContext.setLogLevel("ERROR")

        #input file path
        #"D:\\spark\\Pyspark-Projects\\Intro\\PySpark-Fundamentals\\filter_program.txt"
        filePath = u"D:\\spark\\Pyspark-Projects\\Intro\\PySpark-Fundamentals\\filter_program.txt"
        
        #reading file using spark 
        fileInput = spark.sparkContext.textFile(filePath)
        print((fileInput).collect())
        
        filteredRDD = fileInput.filter(lambda x : 'Py' in x)
        print(type(filteredRDD))

        print("Stopping spark session")
        spark.stop()