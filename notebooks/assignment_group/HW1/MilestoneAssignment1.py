#Importing the data
import os
import  pandas as pd
import numpy as np

url = "https://github.com/SagaraKohsuke/graspp-25S-innovation/blob/main/data/2023_kaku_01-10.xls"
df = pd.read_excel(url, sheet_name="2023_kaku_01-10", skiprows=7)
df.head(2)