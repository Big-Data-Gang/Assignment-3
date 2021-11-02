#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# find all rows with a particular value in a column greater than the average in a csv file using pyspark
def findRowsSpark(country, path):

    spark = SparkSession.builder.appName("findRows").getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.show()
    df = df.filter(df.Country == country)
    # find average of column for all groups of column City
    groupedByCity = df.groupBy("City").agg(F.mean('AverageTemperature'))
    
    # join groupedByCity and df over City column
    joined = df.join(groupedByCity, on = ["City"], how='inner')
    print("joined table")
    joined.show()
    
    # count rows where AverageTemperature is greater than the average
    print("Matched values for given criteria")
    joined = joined.filter(joined.AverageTemperature > joined["avg(AverageTemperature)"])
    joined.show()

    #count the number of occurances of a particular city
    final = joined.groupBy("City").count()
    print("Only whatever is after this is the output>>>>")
    final.show()
    spark.stop()

    # TODO : Print the result as per output format

if __name__ == '__main__':
    # to test, execute the following command
    # $SPARK_HOME/bin/spark-submit task1.py India city_sample.csv > output.txt 

    if len(sys.argv) != 3:
        raise Exception("Re-run the script in this format: task1.py <country> <path>")
        sys.exit(-1)
    findRowsSpark(sys.argv[1], sys.argv[2])