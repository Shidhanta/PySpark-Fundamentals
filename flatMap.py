from pyspark import RDD
from pyspark.sql import SparkSession

# FlatMap -> transformation operation that flattens an RDD/Dataframe
# (array/map/ dataframe columns) after applying the function on every element
# returns a new RDD/Dataframe
if __name__ =="__main__":
        
        spark =  SparkSession \
            .builder \
            .appName("Creating RDD") \
            .master("local[*]") \
            .getOrCreate()

        #only display error messages to console
        spark.sparkContext.setLogLevel("ERROR")
        
        list = ["1,2,3,4,5" , "7,6,5,4,3,2" ]
        print("List : ",list)

        #constructing a RDD of numbers mapping a function that squares element
        listRDD = spark.sparkContext.parallelize(list,1)
        flatMapRDD = listRDD.map(lambda s: s.split(","))

        print(flatMapRDD.collect())