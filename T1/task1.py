#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# find all rows with a particular value in a column greater than the average in a csv file using pyspark
def findRowsSpark(country, path):

    spark = SparkSession.builder.appName("findRows").getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)
    # df.show()
    df = df.filter(df.Country == country)
    # find average of column for all groups of column City
    groupedByCity = df.groupBy("City").agg(F.mean('AverageTemperature'))
    
    # join groupedByCity and df over City column
    joined = df.join(groupedByCity, on = ["City"], how='inner')
    # joined.show()
    
    # count rows where AverageTemperature is greater than the average
    joined = joined.filter(joined["AverageTemperature"] > joined["avg(AverageTemperature)"])
    # joined.show()

    #count the number of occurrences of a particular city
    final = joined.groupBy("City").count().orderBy("City")
    # final.show()
    # print final as tab spaced columns and rows
    for row in final.collect():
    	print(row[0] + "\t" +str(row[1]))
    
    spark.stop()

    # TODO : Print the result as per output format
def main():
    """
    to test, execute the following command
    
    ! to run the code
        `$SPARK_HOME/bin/spark-submit task1.py India ../city_sample_5percent.csv > ourcode_5percent_india.txt`
    
    ! to see the difference in expected output and ourcode
        `diff ourcode_5percent_india.txt t1_output_5percent_india.txt`
    """
    if len(sys.argv) != 3:
        raise Exception("Re-run the script in this format: task1.py <country> <path>")
        sys.exit(-1)
    findRowsSpark(sys.argv[1], sys.argv[2])
if __name__ == '__main__':
    main()