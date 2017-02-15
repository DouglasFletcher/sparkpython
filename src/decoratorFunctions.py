
import time

def timerFn(func):
	# =======================
	# purpose: time functions
	# date: 2017.02.14
	# =======================
	def timerFnCall(*arg):
		t = time.clock()
		res = func(*arg)
		diff = format(time.clock()-t, ".2f")
		print(func.__name__, "took:", diff, "seconds")
		return res
	return timerFnCall

