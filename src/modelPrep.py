

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
		  StructField("rowno", IntegerType(), False)
		, StructField("SeriousDlqin2yrs", IntegerType(), True)
		, StructField("RevolvingUtilizationOfUnsecuredLines", FloatType(), True)
		, StructField("age", IntegerType(), True)
		, StructField("NumberOfTime3059DaysPastDueNotWorse", IntegerType(), True)
		, StructField("DebtRatio", FloatType(), True)
		, StructField("MonthlyIncome", IntegerType(), True)
		, StructField("NumberOfOpenCreditLinesAndLoans", IntegerType(), True)
		, StructField("NumberOfTimes90DaysLate", IntegerType(), True)
		, StructField("NumberRealEstateLoansOrLines", IntegerType(), True)
		, StructField("NumberOfTime6089DaysPastDueNotWorse", IntegerType(), True)
		, StructField("NumberOfDependents", IntegerType(), True)
	])
	# read data
	sparkDf = spark.read.csv(
		FILEIN
		, schema = schema
		, header = "true"
		, nanValue = "NA"
	)
	# create training & test
	training = sparkDf.filter(sparkDf.rowno <= 125000)
	#testing = sparkDf.filter(sparkDf.rowno > 125000)
	# both datasets
	output = [training]
	# output
	return output
	


@timerFn
def createTrans(sparkDF):
	# ===========================
	# douglas fletcher
	# purpose: create data 
	# transformations 
	# input: 
	# 	sparkDF type sparkDF
	# output: 
	# 	sparkDFTrans type sparkDF
	# ===========================
	sparkDFTrans = sparkDF



	'''
	x$UnknownNumberOfDependents <- ifelse(isNull(x$NumberOfDependents), 1, 0)
	x$UnknownMonthlyIncome <- ifelse(isNull(x$MonthlyIncome), 1, 0)

	x$NoDependents <- ifelse(x$NumberOfDependents == 0, 1, 0)
	x$NumberOfDependents <- ifelse(x$UnknownNumberOfDependents==1, 0, x$NumberOfDependents)

	x$NoIncome <- ifelse(x$MonthlyIncome == 0, 1, 0)
	x$NoIncome <- ifelse(isNull(x$NoIncome), 0, x$NoIncome)
	x$MonthlyIncome <- ifelse(x$UnknownMonthlyIncome==1, 0, x$MonthlyIncome)

	x$ZeroDebtRatio <- ifelse(x$DebtRatio == 0, 1, 0)
	x$UnknownIncomeDebtRatio <- x$DebtRatio
	
	x$UnknownIncomeDebtRatio <- ifelse(x$UnknownMonthlyIncome == 0, 0, x$UnknownIncomeDebtRatio)
	x$DebtRatio <- ifelse(x$UnknownMonthlyIncome == 1, 0, x$DebtRatio)

	x$WeirdRevolvingUtilization <- x$RevolvingUtilizationOfUnsecuredLines
	x$WeirdRevolvingUtilization <- ifelse((log(x$RevolvingUtilizationOfUnsecuredLines) > 3), x$WeirdRevolvingUtilization, 0)
	x$ZeroRevolvingUtilization <- ifelse(x$RevolvingUtilizationOfUnsecuredLines == 0, 1, 0)
	x$RevolvingUtilizationOfUnsecuredLines <- ifelse(log(x$RevolvingUtilizationOfUnsecuredLines) > 3, 0, x$RevolvingUtilizationOfUnsecuredLines)

	x$LogDebt <- log(ifelse(x$MonthlyIncome <= 1, 1, x$MonthlyIncome) * x$DebtRatio)
	x$LogDebt <- ifelse(isNull(x$LogDebt), 0, x$LogDebt)
	x$RevolvingLines <- x$NumberOfOpenCreditLinesAndLoans - x$NumberRealEstateLoansOrLines

	x$HasRevolvingLines <- ifelse(x$RevolvingLines > 0, 1, 0)
	x$HasRealEstateLoans <- ifelse(x$NumberRealEstateLoansOrLines > 0, 1, 0)
	x$HasMultipleRealEstateLoans <- ifelse(x$NumberRealEstateLoansOrLines > 2, 1, 0)
	x$EligibleSS <- ifelse(x$age >= 60, 1, 0)

	x$DTIOver33 <- ifelse(x$NoIncome == 0 & x$DebtRatio > 0.33, 1, 0)
	x$DTIOver43 <- ifelse(x$NoIncome == 0 & x$DebtRatio > 0.43, 1, 0)
	x$DisposableIncome <- (lit(1.0) - x$DebtRatio) * x$MonthlyIncome
	x$DisposableIncome <- ifelse(x$NoIncome==1, 0, x$DisposableIncome)
	x$RevolvingToRealEstate <- x$RevolvingLines / (lit(1) + x$NumberRealEstateLoansOrLines)
	x$NumberOfTime3059DaysPastDueNotWorseLarge <- ifelse(x$NumberOfTime3059DaysPastDueNotWorse > 90, 1, 0)
	x$NumberOfTime3059DaysPastDueNotWorse96 <- ifelse(x$NumberOfTime3059DaysPastDueNotWorse == 96, 1, 0)
	x$NumberOfTime3059DaysPastDueNotWorse98 <- ifelse(x$NumberOfTime3059DaysPastDueNotWorse == 98, 1, 0)
	x$Never3059DaysPastDueNotWorse <- ifelse(x$NumberOfTime3059DaysPastDueNotWorse == 0, 1, 0)
	x$NumberOfTime3059DaysPastDueNotWorse <- ifelse(x$NumberOfTime3059DaysPastDueNotWorse > 90, 0, x$NumberOfTime3059DaysPastDueNotWorse)

	x$NumberOfTime6089DaysPastDueNotWorseLarge <- ifelse(x$NumberOfTime6089DaysPastDueNotWorse > 90, 1, 0)
	x$NumberOfTime6089DaysPastDueNotWorse96 <- ifelse(x$NumberOfTime6089DaysPastDueNotWorse == 96, 1, 0)
	x$NumberOfTime6089DaysPastDueNotWorse98 <- ifelse(x$NumberOfTime6089DaysPastDueNotWorse == 98, 1, 0)
	x$Never6089DaysPastDueNotWorse <- ifelse(x$NumberOfTime6089DaysPastDueNotWorse == 0, 1, 0)
	x$NumberOfTime6089DaysPastDueNotWorse <- ifelse(x$NumberOfTime6089DaysPastDueNotWorse > 90, 0, x$NumberOfTime6089DaysPastDueNotWorse)

	x$NumberOfTimes90DaysLateLarge <- ifelse(x$NumberOfTimes90DaysLate > 90, 1, 0)
	x$NumberOfTimes90DaysLate96 <- ifelse(x$NumberOfTimes90DaysLate == 96, 1, 0)
	x$NumberOfTimes90DaysLate98 <- ifelse(x$NumberOfTimes90DaysLate == 98, 1, 0)
	x$Never90DaysLate <- ifelse(x$NumberOfTimes90DaysLate == 0, 1, 0)
	x$NumberOfTimes90DaysLate <- ifelse(x$NumberOfTimes90DaysLate > 90, 0, x$NumberOfTimes90DaysLate)

	x$IncomeDivBy10 <- ifelse(x$MonthlyIncome %% 10 == 0, 1, 0)
	x$IncomeDivBy100 <- ifelse(x$MonthlyIncome %% 100 == 0, 1, 0)
	x$IncomeDivBy1000 <- ifelse(x$MonthlyIncome %% 1000 == 0, 1, 0)
	x$IncomeDivBy5000 <- ifelse(x$MonthlyIncome %% 5000 == 0, 1, 0)
	x$Weird0999Utilization <- ifelse(x$RevolvingUtilizationOfUnsecuredLines == 0.9999999, 1, 0)
	x$FullUtilization <- ifelse(x$RevolvingUtilizationOfUnsecuredLines == 1, 1, 0)
	x$ExcessUtilization <- ifelse(x$RevolvingUtilizationOfUnsecuredLines > 1, 1, 0)

	x$NumberOfTime3089DaysPastDueNotWorse <- x$NumberOfTime3059DaysPastDueNotWorse + x$NumberOfTime6089DaysPastDueNotWorse
	x$Never3089DaysPastDueNotWorse <- x$Never6089DaysPastDueNotWorse * x$Never3059DaysPastDueNotWorse
	x$NumberOfTimesPastDue <- x$NumberOfTime3059DaysPastDueNotWorse + x$NumberOfTime6089DaysPastDueNotWorse + x$NumberOfTimes90DaysLate
	x$NeverPastDue <- x$Never90DaysLate * x$Never6089DaysPastDueNotWorse * x$Never3059DaysPastDueNotWorse

	x$LogRevolvingUtilizationTimesLines <- log1p(x$RevolvingLines * x$RevolvingUtilizationOfUnsecuredLines)
	x$LogRevolvingUtilizationOfUnsecuredLines <- log(x$RevolvingUtilizationOfUnsecuredLines)
	x$LogRevolvingUtilizationOfUnsecuredLines <- ifelse(isNull(x$LogRevolvingUtilizationOfUnsecuredLines), 0, x$LogRevolvingUtilizationOfUnsecuredLines)
	x$LogRevolvingUtilizationOfUnsecuredLines<- ifelse(isNull(x$LogRevolvingUtilizationOfUnsecuredLines), 0, x$LogRevolvingUtilizationOfUnsecuredLines)
	x$RevolvingUtilizationOfUnsecuredLines <- NULL

	x$DelinquenciesPerLine <- x$NumberOfTimesPastDue / x$NumberOfOpenCreditLinesAndLoans
	x$DelinquenciesPerLine <- ifelse(x$NumberOfOpenCreditLinesAndLoans == 0, 0, x$DelinquenciesPerLine)
	x$MajorDelinquenciesPerLine <- x$NumberOfTimes90DaysLate / x$NumberOfOpenCreditLinesAndLoans
	x$MajorDelinquenciesPerLine <- ifelse(x$NumberOfOpenCreditLinesAndLoans==0, 0, x$MajorDelinquenciesPerLine)
	x$MinorDelinquenciesPerLine <- x$NumberOfTime3089DaysPastDueNotWorse / x$NumberOfOpenCreditLinesAndLoans
	x$MinorDelinquenciesPerLine <- ifelse(x$NumberOfOpenCreditLinesAndLoans == 0, 0, x$MinorDelinquenciesPerLine)

	# Now delinquencies per revolving
	x$DelinquenciesPerRevolvingLine <- x$NumberOfTimesPastDue / x$RevolvingLines
	x$DelinquenciesPerRevolvingLine <- ifelse(x$RevolvingLines == 0, 0, x$DelinquenciesPerRevolvingLine)
	x$MajorDelinquenciesPerRevolvingLine <- x$NumberOfTimes90DaysLate / x$RevolvingLines
	x$MajorDelinquenciesPerRevolvingLine <- ifelse(x$RevolvingLines == 0, 0, x$MajorDelinquenciesPerRevolvingLine)
	x$MinorDelinquenciesPerRevolvingLine <- x$NumberOfTime3089DaysPastDueNotWorse / x$RevolvingLines
	x$MinorDelinquenciesPerRevolvingLine <- ifelse(x$RevolvingLines == 0, 0, x$MinorDelinquenciesPerRevolvingLine)

	x$LogDebtPerLine <- x$LogDebt - log1p(x$NumberOfOpenCreditLinesAndLoans)
	x$LogDebtPerRealEstateLine <- x$LogDebt - log1p(x$NumberRealEstateLoansOrLines)
	x$LogDebtPerPerson <- x$LogDebt - log1p(x$NumberOfDependents)
	x$RevolvingLinesPerPerson <- x$RevolvingLines / (lit(1) + x$NumberOfDependents)
	x$RealEstateLoansPerPerson <- x$NumberRealEstateLoansOrLines / (lit(1) + x$NumberOfDependents)
	x$YearsOfAgePerDependent <- x$age / (lit(1) + x$NumberOfDependents)

	x$LogMonthlyIncome <- log(x$MonthlyIncome)
	x$LogMonthlyIncome <- ifelse((isNull(x$LogMonthlyIncome)) | (isNull(x$LogMonthlyIncome)), 0, x$LogMonthlyIncome)
	x$MonthlyIncome <- NULL
	x$LogIncomePerPerson <- x$LogMonthlyIncome - log1p(x$NumberOfDependents)
	x$LogIncomeAge <- x$LogMonthlyIncome - log1p(x$age)

	x$LogNumberOfTimesPastDue <- log(x$NumberOfTimesPastDue)
	x$LogNumberOfTimesPastDue <- ifelse(isNull(x$LogNumberOfTimesPastDue), 0, x$LogNumberOfTimesPastDue)

	x$LogNumberOfTimes90DaysLate <- log(x$NumberOfTimes90DaysLate)
	x$LogNumberOfTimes90DaysLate <- ifelse(isNull(x$LogNumberOfTimes90DaysLate), 0, x$LogNumberOfTimes90DaysLate)

	x$LogNumberOfTime3059DaysPastDueNotWorse <- log(x$NumberOfTime3059DaysPastDueNotWorse)
	x$LogNumberOfTime3059DaysPastDueNotWorse <- ifelse(isNull(x$LogNumberOfTime3059DaysPastDueNotWorse), 0, x$LogNumberOfTime3059DaysPastDueNotWorse)
	x$LogNumberOfTime6089DaysPastDueNotWorse <- log(x$NumberOfTime6089DaysPastDueNotWorse)
	x$LogNumberOfTime6089DaysPastDueNotWorse <- ifelse(isNull(x$LogNumberOfTime6089DaysPastDueNotWorse), 0, x$LogNumberOfTime6089DaysPastDueNotWorse)

	x$LogRatio90to3059DaysLate <- x$LogNumberOfTimes90DaysLate - x$LogNumberOfTime3059DaysPastDueNotWorse
	x$LogRatio90to6089DaysLate <- x$LogNumberOfTimes90DaysLate - x$LogNumberOfTime6089DaysPastDueNotWorse

	x$AnyOpenCreditLinesOrLoans <- ifelse(x$NumberOfOpenCreditLinesAndLoans > 0, 1, 0)
	x$LogNumberOfOpenCreditLinesAndLoans <- log(x$NumberOfOpenCreditLinesAndLoans)
	x$LogNumberOfOpenCreditLinesAndLoans <- ifelse(isNull(x$LogNumberOfOpenCreditLinesAndLoans), 0, x$LogNumberOfOpenCreditLinesAndLoans)
	x$LogNumberOfOpenCreditLinesAndLoansPerPerson <- x$LogNumberOfOpenCreditLinesAndLoans - log1p(x$NumberOfDependents)

	x$HasDependents <- ifelse(x$NumberOfDependents > 0, 1, 0)
	x$LogHouseholdSize <- log1p(x$NumberOfDependents)
	x$NumberOfDependents <- NULL

	x$LogDebtRatio <- log(x$DebtRatio)
	x$LogDebtRatio <- ifelse(isNull(x$LogDebtRatio), 0, x$LogDebtRatio)
	x$DebtRatio <- NULL
	'''
	'''
	###
	x$LogDebtPerDelinquency <- x$LogDebt - log1p(x$NumberOfTimesPastDue)
	x$LogDebtPer90DaysLate <- x$LogDebt - log1p(x$NumberOfTimes90DaysLate)
	'''
	'''
	x$LogUnknownIncomeDebtRatio <- log(x$UnknownIncomeDebtRatio)
	x$LogUnknownIncomeDebtRatio <- ifelse(isNull(x$LogUnknownIncomeDebtRatio), 0, x$LogUnknownIncomeDebtRatio)
	x$IntegralDebtRatio <- NULL
	x$LogUnknownIncomeDebtRatioPerPerson <- x$LogUnknownIncomeDebtRatio - x$LogHouseholdSize
	'''
	'''
	###
	x$LogUnknownIncomeDebtRatioPerLine <- x$LogUnknownIncomeDebtRatio - log1p(x$NumberOfOpenCreditLinesAndLoans)
	x$LogUnknownIncomeDebtRatioPerRealEstateLine <- x$LogUnknownIncomeDebtRatio - log1p(x$NumberRealEstateLoansOrLines)
	x$LogUnknownIncomeDebtRatioPerDelinquency <- x$LogUnknownIncomeDebtRatio - log1p(x$NumberOfTimesPastDue)
	x$LogUnknownIncomeDebtRatioPer90DaysLate <- x$LogUnknownIncomeDebtRatio - log1p(x$NumberOfTimes90DaysLate)
	'''
	'''
	x$LogNumberRealEstateLoansOrLines <- log(x$NumberRealEstateLoansOrLines)
	x$LogNumberRealEstateLoansOrLines <- ifelse(isNull(x$LogNumberRealEstateLoansOrLines), 0, x$LogNumberRealEstateLoansOrLines)
	x$NumberRealEstateLoansOrLines <- NULL

	x$NumberOfOpenCreditLinesAndLoans <- NULL
	x$NumberOfTimesPastDue <- NULL
	x$NumberOfTimes90DaysLate <- NULL
	x$NumberOfTime3059DaysPastDueNotWorse <- NULL
	x$NumberOfTime6089DaysPastDueNotWorse <- NULL

	x$LowAge <- ifelse(x$age < 18, 1, 0)
	x$Logage <- log(x$age - 17)
	x$Logage <- ifelse(x$LowAge == 1, 0, x$Logage)
	x$age <- NULL
	'''
	return sparkDFTrans



@timerFn
def createRandomForest(sparkDF, NUMTREES, NUMCLASSES, MAXDEPTH):
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
	# create random forest model
	model = RandomForest.trainClassifier(
		  data
		, numTrees = NUMTREES
		, numClasses = NUMCLASSES
		, maxDepth = MAXDEPTH
		, impurity='gini'
		, featureSubsetStrategy="auto"
		, categoricalFeaturesInfo={}
		, seed=42
		#, maxBins=32
	)
	print(model.numTrees())





