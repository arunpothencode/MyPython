import pandas as pd
import os

inppara = pd.read_csv(r"C:\Users\arun.r\Documents\Sharath\names.txt", delimiter=',', encoding='UTF-8', skiprows=[0])


def myfunc(name):
    print("Hello World " + name)


for i in inppara:
    myfunc(i)
