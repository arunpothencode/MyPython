import pandas as pd
import os
import glob

inppara = pd.read_csv(r"C:\Users\arun.r\Documents\Sharath\names.txt")
inpfiles = glob.glob(r"C:\Users\arun.r\Documents\Sharath\f*.txt")
print(inppara)
print(inpfiles)
