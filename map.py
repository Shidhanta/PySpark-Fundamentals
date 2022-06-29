from pyspark.sql import SparkSession

if __name__ =="__main__":
        
        spark =  SparkSession \
            .builder \
            .appName("Creating RDD") \
            .master("local[*]") \
            .getOrCreate()

        #only display error messages to console
        spark.sparkContext.setLogLevel("ERROR")
        
        number_list = [1,2,3,4,5,6]
        print("List of Numbers: ",number_list)

        #constructing a RDD of numbers mapping a function that squares element
        numberRDD = spark.sparkContext.parallelize(number_list,1)
        squareNumber = numberRDD.map(lambda n: n*n)

        print(squareNumber.collect())


