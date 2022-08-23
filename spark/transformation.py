from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    df = spark.read.json(
        "datalake/twitter_aluraonline/extract_date=2022-xx-xx"
    )
    df.printSchema()
    df.show()