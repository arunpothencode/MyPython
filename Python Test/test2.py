import glob, os
import pandas as pd
import pathlib

path = r"C:\Users\arun.r\Documents\Sharath\names.txt"
files = os.listdir(path)
files_txt = [i for i in files if i.endswith('.txt')]
