

# =======
# globals
# =======
PATH = "C:/Users/douglas.fletcher/Documents/projects/"
PROJECTLOC = PATH + "/creditscore_spark/creditscoring_py"
PYLIBLOCAL =  PROJECTLOC + "/src/"
RAWDATA = PATH + "/creditscore_spark/01_data/cstraining_kaggle_test.csv"


# ============
# dependencies
# ============
import sys as sys

# local functions
sys.path.append(PYLIBLOCAL)
from init_spark import sparkInit
from modelPrep import importData, createTrans, createRandomForest

# =================
# start application
# =================

if __name__ == "__main__":
	# creat spark instance
	spark = sparkInit(PROJECTLOC)

	# read rawdata
	rawdataList = importData(spark, RAWDATA)

	# prep data
	rawsData = rawdataList[0]
	prepData = createTrans(rawsData)

	# create model
	NUMTREES = 2
	NUMCLASSES = 2
	MAXDEPTH = 3
	#createRandomForest(prepData, NUMTREES, NUMCLASSES, MAXDEPTH)
