

# libraries
import sys as sys
# local functions
from decoratorFunctions import timerFn
# spark dependencies
sys.path.append("C:/spark-2.1.0-bin-hadoop2.7/python/")
from pyspark.sql.types import *
from pyspark.sql.readwriter import DataFrameReader
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest

# functions
from pyspark.sql.functions import when
from pyspark.sql.functions import log10
from pyspark.sql.functions import log1p

@timerFn
def importData(spark, FILEIN):
	# ===============================
	# douglas fletcher
	# purpose: read data 
	# configuration
	# input:
	# 	sqlContext type pyspark
	# 	fileIn type String
	# output: 
	# 	output type list of sparkDF
	# ===============================
	# create schema
	schema = StructType([
		  StructField("rowno", IntegerType(), True)
		, StructField("SeriousDlqin2yrs", FloatType(), False)
		, StructField("RevolvingUtilizationOfUnsecuredLines", FloatType(), False)
		, StructField("age", IntegerType(), False)
		, StructField("NumberOfTime3059DaysPastDueNotWorse", IntegerType(), False)
		, StructField("DebtRatio", FloatType(), False)
		, StructField("MonthlyIncome", IntegerType(), False)
		, StructField("NumberOfOpenCreditLinesAndLoans", IntegerType(), False)
		, StructField("NumberOfTimes90DaysLate", IntegerType(), False)
		, StructField("NumberRealEstateLoansOrLines", IntegerType(), False)
		, StructField("NumberOfTime6089DaysPastDueNotWorse", IntegerType(), False)
		, StructField("NumberOfDependents", IntegerType(), False)
	])
	# read data
	sparkDf = spark.read.csv(
		FILEIN
		, schema=schema
		, header="true"
		, nullValue="NA"
	)
	# create training & test
	training = sparkDf.filter(sparkDf.rowno <= 125000)
	testing = sparkDf.filter(sparkDf.rowno > 125000)
	# both datasets
	output = [training, testing]
	# output
	return output
	


@timerFn
def createTrans01(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfDependents.isNull(), 1)
		  .otherwise(0)
		  .alias("UnknownNumberOfDependents")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.MonthlyIncome.isNull(), 1)
		  .otherwise(0)
		  .alias("UnknownMonthlyIncome")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfDependents == 0, 1)
		  .otherwise(0)
		  .alias("NoDependents")
	)
	sparkDFTrans = sparkDFTrans.withColumn("NumberOfDependents"
		, when(sparkDFTrans.UnknownNumberOfDependents == 1, 0)
		  .otherwise(sparkDFTrans.NumberOfDependents)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.MonthlyIncome == 0, 1)
		  .otherwise(0)
		  .alias("NoIncome")
	)
	sparkDFTrans = sparkDFTrans.withColumn("NoIncome"
		, when(sparkDFTrans.NoIncome.isNull(), 0).otherwise(sparkDFTrans.NoIncome)
	)
	sparkDFTrans = sparkDFTrans.withColumn("MonthlyIncome"
		, when(sparkDFTrans.UnknownMonthlyIncome == 1, 0)
		  .otherwise(sparkDFTrans.MonthlyIncome)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.DebtRatio == 0, 1)
		  .otherwise(0)
		  .alias("ZeroDebtRatio")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.DebtRatio == 0, 0)
		  .otherwise(sparkDFTrans.DebtRatio)
		  .alias("UnknownIncomeDebtRatio")
	)
	sparkDFTrans = sparkDFTrans.withColumn("DebtRatio"
		, when(sparkDFTrans.UnknownMonthlyIncome == 1, 0)
		  .otherwise(sparkDFTrans.DebtRatio)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(log10(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines) > 3
			, sparkDFTrans.RevolvingUtilizationOfUnsecuredLines)
		.otherwise(0)
		.alias("WeirdRevolvingUtilization")
	)	
	return sparkDFTrans



@timerFn
def createTrans02(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines == 0, 1)
		  .otherwise(0)
		  .alias("ZeroRevolvingUtilization")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("RevolvingUtilizationOfUnsecuredLines"
		, when(log10(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines) > 3, 0)
		  .otherwise(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.MonthlyIncome <= 1, 1)
		  .otherwise(sparkDFTrans.MonthlyIncome)
		  .alias("LogDebt")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogDebt"
		, log10(sparkDFTrans.LogDebt * sparkDFTrans.DebtRatio)
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogDebt"
		, when(sparkDFTrans.LogDebt.isNull(), 0)
		  .otherwise(sparkDFTrans.LogDebt)
	)	
	sparkDFTrans = sparkDFTrans.withColumn("RevolvingLines"
		, sparkDFTrans.NumberOfOpenCreditLinesAndLoans - sparkDFTrans.NumberRealEstateLoansOrLines
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.RevolvingLines > 0, 1)
		  .otherwise(0)
		  .alias("HasRevolvingLines")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberRealEstateLoansOrLines > 0, 1)
		  .otherwise(0)
		  .alias("HasRealEstateLoans")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberRealEstateLoansOrLines > 2, 1)
		  .otherwise(0)
		  .alias("HasMultipleRealEstateLoans")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.age > 60, 1)
		  .otherwise(0)
		  .alias("EligibleSS")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when((sparkDFTrans.NoIncome == 0) & (sparkDFTrans.DebtRatio > 0.33), 1)
		  .otherwise(0)
		  .alias("DTIOver33")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when((sparkDFTrans.NoIncome == 0) & (sparkDFTrans.DebtRatio > 0.43), 1)
		  .otherwise(0)
		  .alias("DTIOver43")
	)	
	return sparkDFTrans



@timerFn
def createTrans03(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, ((1 - sparkDFTrans.DebtRatio) * sparkDFTrans.MonthlyIncome)  
		  .alias("DisposableIncome")
	)
	sparkDFTrans = sparkDFTrans.withColumn("DisposableIncome"
		, when(sparkDFTrans.NoIncome==1, 0).otherwise(sparkDFTrans.DisposableIncome)
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.RevolvingLines / (1 + sparkDFTrans.NumberRealEstateLoansOrLines))  
		  .alias("RevolvingToRealEstate")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse>90, 1).otherwise(0) 
		  .alias("NumberOfTime3059DaysPastDueNotWorseLarge")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse==96, 1).otherwise(0) 
		  .alias("NumberOfTime3059DaysPastDueNotWorse96")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse==98, 1).otherwise(0) 
		  .alias("NumberOfTime3059DaysPastDueNotWorse98")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse==0, 1).otherwise(0) 
		  .alias("Never3059DaysPastDueNotWorse")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("NumberOfTime3059DaysPastDueNotWorse"
		, when(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse>90, 1)
		  .otherwise(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse) 
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse>90, 1).otherwise(0) 
		  .alias("NumberOfTime6089DaysPastDueNotWorseLarge")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse==96, 1).otherwise(0) 
		  .alias("NumberOfTime6089DaysPastDueNotWorse96")
	)
	return sparkDFTrans



@timerFn
def createTrans04(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse==98, 1).otherwise(0) 
		  .alias("NumberOfTime6089DaysPastDueNotWorse98")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse==0, 1).otherwise(0) 
		  .alias("Never6089DaysPastDueNotWorse")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("NumberOfTime6089DaysPastDueNotWorse"
		, when(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse>90, 0)
		  .otherwise(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse) 
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTimes90DaysLate>90, 1).otherwise(0) 
		  .alias("NumberOfTimes90DaysLateLarge")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTimes90DaysLate==96, 1).otherwise(0) 
		  .alias("NumberOfTimes90DaysLate96")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTimes90DaysLate==98, 1).otherwise(0) 
		  .alias("NumberOfTimes90DaysLate98")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfTimes90DaysLate==0, 1).otherwise(0) 
		  .alias("Never90DaysLate")
	)
	sparkDFTrans = sparkDFTrans.withColumn("NumberOfTimes90DaysLate"
		, when(sparkDFTrans.NumberOfTimes90DaysLate>90, 0)
		  .otherwise(sparkDFTrans.NumberOfTimes90DaysLate) 
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when((sparkDFTrans.MonthlyIncome % 10) == 0, 1).otherwise(0) 
		  .alias("IncomeDivBy10")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when((sparkDFTrans.MonthlyIncome % 100) == 0, 1).otherwise(0) 
		  .alias("IncomeDivBy100")
	)	
	return sparkDFTrans




@timerFn
def createTrans05(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, when((sparkDFTrans.MonthlyIncome % 1000) == 0, 1).otherwise(0) 
		  .alias("IncomeDivBy1000")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when((sparkDFTrans.MonthlyIncome % 5000) == 0, 1).otherwise(0) 
		  .alias("IncomeDivBy5000")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines==0.9999999, 1).otherwise(0) 
		  .alias("Weird0999Utilization")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines == 1, 1).otherwise(0) 
		  .alias("FullUtilization")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines > 1, 1).otherwise(0) 
		  .alias("ExcessUtilization")
	)
	sparkDFTrans = sparkDFTrans.withColumn("NumberOfTime3089DaysPastDueNotWorse"
		, sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse + sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse
	)	
	sparkDFTrans = sparkDFTrans.withColumn("Never3089DaysPastDueNotWorse"
		, sparkDFTrans.Never6089DaysPastDueNotWorse * sparkDFTrans.Never3059DaysPastDueNotWorse
	)
	sparkDFTrans = sparkDFTrans.withColumn("NumberOfTimesPastDue"
		, sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse+sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse+sparkDFTrans.NumberOfTimes90DaysLate
	)	
	sparkDFTrans = sparkDFTrans.withColumn("NeverPastDue"
		, sparkDFTrans.Never90DaysLate + sparkDFTrans.Never6089DaysPastDueNotWorse * sparkDFTrans.Never3059DaysPastDueNotWorse
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (log1p(sparkDFTrans.RevolvingLines * sparkDFTrans.RevolvingUtilizationOfUnsecuredLines))
		  .alias("LogRevolvingUtilizationTimesLines")
	)
	return sparkDFTrans



@timerFn
def createTrans06(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, (log10(sparkDFTrans.RevolvingUtilizationOfUnsecuredLines))
		  .alias("LogRevolvingUtilizationOfUnsecuredLines")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogRevolvingUtilizationOfUnsecuredLines"
		, when(sparkDFTrans.LogRevolvingUtilizationOfUnsecuredLines.isNull(), 0)
		  .otherwise(sparkDFTrans.LogRevolvingUtilizationOfUnsecuredLines)
	)
	sparkDFTrans = sparkDFTrans.drop("RevolvingUtilizationOfUnsecuredLines")
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberOfTimesPastDue / sparkDFTrans.NumberOfOpenCreditLinesAndLoans)
		  .alias("DelinquenciesPerLine")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("DelinquenciesPerLine"
		, when(sparkDFTrans.NumberOfOpenCreditLinesAndLoans == 0, 0)
		  .otherwise(sparkDFTrans.DelinquenciesPerLine)
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberOfTimes90DaysLate / sparkDFTrans.NumberOfOpenCreditLinesAndLoans)
		  .alias("MajorDelinquenciesPerLine")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("MajorDelinquenciesPerLine"
		, when(sparkDFTrans.NumberOfOpenCreditLinesAndLoans == 0, 0)
		  .otherwise(sparkDFTrans.MajorDelinquenciesPerLine)
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberOfTime3089DaysPastDueNotWorse / sparkDFTrans.NumberOfOpenCreditLinesAndLoans)
		  .alias("MinorDelinquenciesPerLine")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("MinorDelinquenciesPerLine"
		, when(sparkDFTrans.NumberOfOpenCreditLinesAndLoans == 0, 0)
		  .otherwise(sparkDFTrans.MinorDelinquenciesPerLine)
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberOfTimesPastDue / sparkDFTrans.RevolvingLines)
		  .alias("DelinquenciesPerRevolvingLine")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("DelinquenciesPerRevolvingLine"
		, when(sparkDFTrans.RevolvingLines == 0, 0)
		  .otherwise(sparkDFTrans.DelinquenciesPerRevolvingLine)
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberOfTimes90DaysLate / sparkDFTrans.RevolvingLines)
		  .alias("MajorDelinquenciesPerRevolvingLine")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("MajorDelinquenciesPerRevolvingLine"
		, when(sparkDFTrans.RevolvingLines == 0, 0)
		  .otherwise(sparkDFTrans.MajorDelinquenciesPerRevolvingLine)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberOfTime3089DaysPastDueNotWorse / sparkDFTrans.RevolvingLines)
		  .alias("MinorDelinquenciesPerRevolvingLine")
	)	
	sparkDFTrans = sparkDFTrans.withColumn("MinorDelinquenciesPerRevolvingLine"
		, when(sparkDFTrans.RevolvingLines == 0, 0)
		  .otherwise(sparkDFTrans.MinorDelinquenciesPerRevolvingLine)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogDebt - log1p(sparkDFTrans.NumberOfOpenCreditLinesAndLoans))
		  .alias("LogDebtPerLine")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogDebt - log1p(sparkDFTrans.NumberRealEstateLoansOrLines))
		  .alias("LogDebtPerRealEstateLine")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogDebt - log1p(sparkDFTrans.NumberOfDependents))
		  .alias("LogDebtPerPerson")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.RevolvingLines /(1+sparkDFTrans.NumberOfDependents))
		  .alias("RevolvingLinesPerPerson")
	)	
	return sparkDFTrans



@timerFn
def createTrans07(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.NumberRealEstateLoansOrLines /(1+sparkDFTrans.NumberOfDependents))
		  .alias("RealEstateLoansPerPerson")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.age /(1+sparkDFTrans.NumberOfDependents))
		  .alias("YearsOfAgePerDependent")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, log10(sparkDFTrans.MonthlyIncome)
		  .alias("LogMonthlyIncome")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogMonthlyIncome"
		, when((sparkDFTrans.LogMonthlyIncome.isNull()) | (sparkDFTrans.LogMonthlyIncome.isNull()), 0)
		  .otherwise(sparkDFTrans.LogMonthlyIncome)
	)
	sparkDFTrans = sparkDFTrans.drop("MonthlyIncome")
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogMonthlyIncome - log1p(sparkDFTrans.NumberOfDependents))
		  .alias("LogIncomePerPerson")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogMonthlyIncome - log1p(sparkDFTrans.age))
		  .alias("LogIncomeAge")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, log10(sparkDFTrans.NumberOfTimesPastDue)
		  .alias("LogNumberOfTimesPastDue")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogNumberOfTimesPastDue"
		, when(sparkDFTrans.LogNumberOfTimesPastDue.isNull(), 0)
		  .otherwise(sparkDFTrans.LogNumberOfTimesPastDue)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, log10(sparkDFTrans.NumberOfTimes90DaysLate)
		  .alias("LogNumberOfTimes90DaysLate")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogNumberOfTimes90DaysLate"
		, when(sparkDFTrans.LogNumberOfTimes90DaysLate.isNull(), 0)
		  .otherwise(sparkDFTrans.LogNumberOfTimes90DaysLate)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, log10(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse)
		  .alias("LogNumberOfTime3059DaysPastDueNotWorse")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogNumberOfTime3059DaysPastDueNotWorse"
		, when(sparkDFTrans.LogNumberOfTime3059DaysPastDueNotWorse.isNull(), 0)
		  .otherwise(sparkDFTrans.LogNumberOfTime3059DaysPastDueNotWorse)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, log10(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse)
		  .alias("LogNumberOfTime6089DaysPastDueNotWorse")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogNumberOfTime6089DaysPastDueNotWorse"
		, when(sparkDFTrans.LogNumberOfTime6089DaysPastDueNotWorse.isNull(), 0)
		  .otherwise(sparkDFTrans.LogNumberOfTime6089DaysPastDueNotWorse)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogNumberOfTimes90DaysLate - sparkDFTrans.LogNumberOfTime3059DaysPastDueNotWorse)
		  .alias("LogRatio90to3059DaysLate")
	)	
	return sparkDFTrans



@timerFn
def createTrans08(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogNumberOfTimes90DaysLate - sparkDFTrans.LogNumberOfTime6089DaysPastDueNotWorse)
		  .alias("LogRatio90to6089DaysLate")
	)	
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfOpenCreditLinesAndLoans > 0, 1).otherwise(0) 
		  .alias("AnyOpenCreditLinesOrLoans")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (log10(sparkDFTrans.NumberOfOpenCreditLinesAndLoans))
		  .alias("LogNumberOfOpenCreditLinesAndLoans")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogNumberOfOpenCreditLinesAndLoans"
		, when(sparkDFTrans.LogNumberOfOpenCreditLinesAndLoans.isNull(), 0)
		  .otherwise(sparkDFTrans.LogNumberOfOpenCreditLinesAndLoans)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogNumberOfOpenCreditLinesAndLoans - log1p(sparkDFTrans.NumberOfDependents))
		  .alias("LogNumberOfOpenCreditLinesAndLoansPerPerson")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.NumberOfDependents > 0, 1).otherwise(0) 
		  .alias("HasDependents")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, log1p(sparkDFTrans.NumberOfDependents) 
		  .alias("LogHouseholdSize")
	)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberOfDependents)
	sparkDFTrans = sparkDFTrans.select("*"
		, log10(sparkDFTrans.DebtRatio) 
		  .alias("LogDebtRatio")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogDebtRatio"
		, when(sparkDFTrans.LogDebtRatio.isNull(), 0)
		  .otherwise(sparkDFTrans.LogDebtRatio)
	)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.DebtRatio)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogDebt - log1p(sparkDFTrans.NumberOfTimesPastDue)) 
		  .alias("LogDebtPerDelinquency")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogDebt - log1p(sparkDFTrans.NumberOfTimes90DaysLate)) 
		  .alias("LogDebtPer90DaysLate")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (log10(sparkDFTrans.UnknownIncomeDebtRatio)) 
		  .alias("LogUnknownIncomeDebtRatio")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogUnknownIncomeDebtRatio"
		, when(sparkDFTrans.LogUnknownIncomeDebtRatio.isNull(), 0)
		  .otherwise(sparkDFTrans.LogUnknownIncomeDebtRatio)
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogUnknownIncomeDebtRatio - sparkDFTrans.LogHouseholdSize) 
		  .alias("LogUnknownIncomeDebtRatioPerPerson")
	)
	return sparkDFTrans



@timerFn
def createTrans09(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations (10 at a time) 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogUnknownIncomeDebtRatio - log1p(sparkDFTrans.NumberOfOpenCreditLinesAndLoans)) 
		  .alias("LogUnknownIncomeDebtRatioPerLine")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogUnknownIncomeDebtRatio - log1p(sparkDFTrans.NumberRealEstateLoansOrLines)) 
		  .alias("LogUnknownIncomeDebtRatioPerRealEstateLine")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogUnknownIncomeDebtRatio - log1p(sparkDFTrans.NumberOfTimesPastDue)) 
		  .alias("LogUnknownIncomeDebtRatioPerDelinquency")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (sparkDFTrans.LogUnknownIncomeDebtRatio - log1p(sparkDFTrans.NumberOfTimes90DaysLate)) 
		  .alias("LogUnknownIncomeDebtRatioPer90DaysLate")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (log10(sparkDFTrans.NumberRealEstateLoansOrLines)) 
		  .alias("LogNumberRealEstateLoansOrLines")
	)
	sparkDFTrans = sparkDFTrans.withColumn("LogNumberRealEstateLoansOrLines"
		, when(sparkDFTrans.LogNumberRealEstateLoansOrLines.isNull(), 0)
		  .otherwise(sparkDFTrans.LogNumberRealEstateLoansOrLines)
	)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberRealEstateLoansOrLines)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberOfOpenCreditLinesAndLoans)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberOfTimesPastDue)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberOfTimes90DaysLate)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberOfTime3059DaysPastDueNotWorse)
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.NumberOfTime6089DaysPastDueNotWorse)
	sparkDFTrans = sparkDFTrans.select("*"
		, when(sparkDFTrans.age < 18, 1).otherwise(0) 
		  .alias("LowAge")
	)
	sparkDFTrans = sparkDFTrans.select("*"
		, (log10(sparkDFTrans.age - 17)) 
		  .alias("Logage")
	)	
	sparkDFTrans = sparkDFTrans.drop(sparkDFTrans.age)	
	return sparkDFTrans



@timerFn
def transDataInfo(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: dataset output 
	# input: 
	# 	sparkDF type sparkDF
	# ===========================
	print("\n dataset number of rows..")
	print(sparkDF.count())



@timerFn
def createRandomForest(sparkDF, NUMTREES, NUMCLASSES):
	# ===========================
	# douglas fletcher
	# purpose: create random 
	# forest model
	# input: 
	# 	spark type sparkSession
	# 	sparkDF type sparkDF
	# output: 
	# ===========================
	# create labelled point rdd
	data = sparkDF.rdd.map(
		lambda row: LabeledPoint(row["SeriousDlqin2yrs"], list(row[2:]))
	)
	# Unknown Bug fix
	val = data.collect()[1]
	# create random forest model
	model = RandomForest.trainClassifier(
		  data
		, numTrees = NUMTREES
		, numClasses = NUMCLASSES
		#, maxDepth = MAXDEPTH
		, impurity='gini'
		, featureSubsetStrategy="auto"
		, categoricalFeaturesInfo={}
		, seed=42
		, maxBins=32
	)




