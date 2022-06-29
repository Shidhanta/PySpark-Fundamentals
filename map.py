from pyspark.sql import SparkSession

# Map -> RDD transformation used to apply a transformation function on 
# every element of a dataframe/RDD and returns a new RDD

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


