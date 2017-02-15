
# libraries
import sys as sys
import os as os
# local functions
from decoratorFunctions import timerFn
# spark dependencies
sys.path.append("C:/spark-2.1.0-bin-hadoop2.7/python/")
from pyspark.sql.session import SparkSession


@timerFn
def sparkInit(SQLWAREHOUSELOC):
	# ===============================
	# douglas fletcher
	# purpose: add spark init 
	# configuration
	# input:
	# output: 
	# 	spark out type sparkSession
	# ===============================
	# configure pyspark
	os.environ["SPARK_HOME"] = "C:/spark-2.1.0-bin-hadoop2.7/"
	os.environ["HADOOP_HOME"] = "C:/winutils/"
	# spark session
	spark = SparkSession.builder \
		.master("local[*]") \
		.appName("riskapp") \
		.config("spark.sql.warehouse.dir", SQLWAREHOUSELOC) \
		.config("spark.driver.memory", "4g") \
		.config("spark.executor.memory", "4g") \
		.config("spark.num.executors", "8") \
		.getOrCreate()
	# logging config
	logger = spark._jvm.org.apache.log4j
	logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)
	return spark

