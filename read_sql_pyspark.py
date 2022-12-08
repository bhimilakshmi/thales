from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *

#function to calculate number of seconds from number of days
days = lambda i: i * 86400
spark = SparkSession.builder.\
    config("spark.jars", r"C:\Users\kasir\OneDrive\Desktop\thales\Lib\mysql-connector-j-8.0.31.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()

wine_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/thales") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "tickers") \
    .option("user", "root").option("password", "12345678").load()
wine_df.printSchema()

wine_df.show(300,truncate=False)


days1_window  = Window.partitionBy(col('ticker')).orderBy(col("date").cast('long')).rangeBetween(-days(1), 0)

#20days
days20_window  = Window.partitionBy(col('ticker')).orderBy(col("date").cast('long')).rangeBetween(-days(19), 0)

days50_window  = Window.partitionBy(col('ticker')).orderBy(col("date").cast('long')).rangeBetween(-days(49), 0)

days200_window  = Window.partitionBy(col('ticker')).orderBy(col("date").cast('long')).rangeBetween(-days(199), 0)

days20_window_df = wine_df.withColumn('rolling_average', avg("close").over(days20_window))
days50_window_df = wine_df.withColumn('rolling_average', avg("close").over(days50_window))
days200_window_df = wine_df.withColumn('rolling_average', avg("close").over(days200_window))

days2_window = wine_df.withColumn('rolling_average', avg("close").over(days1_window))


days2_window.select("date","close","rolling_average").write.csv('2days')

