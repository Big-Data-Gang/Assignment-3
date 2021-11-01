#!/usr/bin/env python3

# find all rows with a particular value in a column greater than the average in a csv file using pyspark
def findRowsSpark(country, path):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = SparkSession.builder.appName("findRows").getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.show()
    # find average of column for all groups of column 2
    groupedByCity = df.groupBy("City").agg(F.mean('AverageTemperature')).show()
    # join groupedByCity and df over City column
    joined = df.join(groupedByCity, df.City == groupedByCity.City and df.Country == country)
    # count rows where AverageTemperature is greater than the average
    df.filter(df.AverageTemperature > joined.avg(joined.AverageTemperature)).show()
    joined.filter(joined.AverageTemperature > joined.avg).show()    
    spark.stop()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: task1.py <country> <path>", file=sys.stderr)
        sys.exit(-1)
    findRowsSpark(sys.argv[1], sys.argv[2])