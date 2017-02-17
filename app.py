

# =======
# globals
# =======
PATH = "C:/Users/douglas.fletcher/Documents/projects/"
PROJECTLOC = PATH + "/creditscore_spark/creditscoring_py"
PYLIBLOCAL =  PROJECTLOC + "/src/"
#RAWDATA = PATH + "/creditscore_spark/01_data/cstraining_kaggle_test.csv"
RAWDATA = PATH + "/creditscore_spark/01_data/cstraining_kaggle.csv"

# ============
# dependencies
# ============
import sys as sys

# local functions
sys.path.append(PYLIBLOCAL)
from init_spark import sparkInit
from modelPrep import *

# =================
# start application
# =================

if __name__ == "__main__":
	# creat spark instance
	print("\n__init spark ...")
	spark = sparkInit(PROJECTLOC)

	# read rawdata
	print("\nreading data ...")	
	rawdataList = importData(spark, RAWDATA)

	# training data
	rawsData = rawdataList[0]

	# prep data
	print("\npreparing data ...")
	prepData = createTrans01(rawsData)
	prepData = createTrans02(prepData)
	prepData = createTrans03(prepData)
	prepData = createTrans04(prepData)
	prepData = createTrans05(prepData)
	prepData = createTrans06(prepData)
	prepData = createTrans07(prepData)
	prepData = createTrans08(prepData)
	prepData = createTrans09(prepData)
	
	# dataset overview
	transDataInfo(prepData)

	# create model
	NUMTREES = 200
	NUMCLASSES = 2
	print("\ncreating model ...")
	#MAXDEPTH = 10
	#createRandomForest(prepData, NUMTREES, NUMCLASSES, MAXDEPTH)
	createRandomForest(prepData, NUMTREES, NUMCLASSES)