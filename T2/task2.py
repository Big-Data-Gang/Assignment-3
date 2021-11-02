from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

def exceedAvg(city_path, global_path):
    spark = SparkSession.builder.appName("exceedAvg").getOrCreate()

    #Read the 2 csvs as dfs
    city_df = spark.read.csv(city_path, header=True, inferSchema=True)
    global_df = spark.read.csv(global_path, header=True, inferSchema=True) 

    city_df.show()
    global_df.show()

    # Group by both Date and Country to get max temp for each country on each date
    grouped = city_df.groupBy("dt", "Country").agg(F.max("AverageTemperature").alias('max_temp'))
    grouped.show()

    # Join with global df on date
    joined = grouped.join(global_df, on = 'dt', how = 'inner')
    joined.show()

    # Filter rows where max for each country on a date > global temp on that date
    exceeds = joined.where(joined['max_temp'] > joined['LandAverageTemperature'])
    exceeds.show()

    # After filter, group by country and count 
    result = exceeds.groupBy('Country').count()
    result.show()
    spark.stop()

if __name__ == '__main__':
    # to test, execute the following command
    # $SPARK_HOME/bin/spark-submit task2.py ../city_sample.csv ../global_sample.csv > output.txt 
    if len(sys.argv) != 3:
        raise Exception("Re-run the script in this format: task1.py <country> <path>")
        sys.exit(-1)

    exceedAvg(sys.argv[1], sys.argv[2])