
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

    numberList = [1,2,3,4,5]
    print("Python list of numbers: ",numberList)
    print(type(numberList))

    print("Creating first RDD from number list")

    #converting python object into RDD
    # the number is the number of partitions of the data  
    numberRDD = spark.sparkContext.parallelize(numberList,3) 
    print(type(numberRDD))
    
    #collect collects RDD operation results and is a python object 
    print(numberRDD.collect())

    stringList = [ "A","B","C","X"]
    print("String list of names: ",stringList)
    print(type(stringList))

    stringRDD = spark.sparkContext.parallelize(stringList,3)
    print(type(stringRDD))

    print(stringRDD.collect())

    print("Stopping Spark Session")
    spark.stop()
