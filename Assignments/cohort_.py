from datetime import datetime
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col

date = "1-12-2019"
noOfWeeks = 3
sparkSession = SparkSession \
    .builder \
    .appName("CohortsAssignment") \
    .master("local") \
    .getOrCreate()

CohortsDataDF = sparkSession.read.format("csv")\
    .option("header",True)\
    .option("delimiter",",")\
    .option("inferSchema",True)\
    .option("nullValue","NA")\
    .option("path", "/home/rita/Documents/Spark/Assignments/DE_Cohorts_Assignment_Data.csv")\
    .load()

start_date = datetime(2019, 12, 1, 0, 0, 0)

days_since_1970_to_start_date =int(time.mktime(start_date.timetuple())/86400)
offset_days = days_since_1970_to_start_date % 7


print("hourly")
data = CohortsDataDF.groupBy(window(CohortsDataDF["timestamp"],"1 hour"),"user_id").count()
hourly = data.groupBy(window(data["window.start"],"1 hour"),"user_id").avg().alias("hourly average").show()
print("daily")
data = CohortsDataDF.groupBy(window(CohortsDataDF["timestamp"],"1 days"),"user_id").count()
daily = data.groupBy(window(data["window.start"],"1 days"),"user_id").avg().alias("daily average").show()
print("weekly")
data = CohortsDataDF.groupBy(window(CohortsDataDF["timestamp"],"1 week"),"user_id").count()
weekly = data.groupBy(window(data["window.start"],"1 week"),"user_id").avg().alias("weekly average").show()


sparkSession.stop()
