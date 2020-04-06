import  os
import sys
os.path.dirname(os.path.realpath(__file__))
file = "main.py"
cmd = "spark-submit --master local[2] /"+file

sys.command(cmd)