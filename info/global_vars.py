from datetime import date
import math


SUB_PATH     =  r'xxx\Account Submission\Master Submission Sheet - V3.xlsx'
TODAY        =  date.today()
QTR_DTE      =  "Q%d_%d" % (math.ceil(TODAY.month / 3), TODAY.year)