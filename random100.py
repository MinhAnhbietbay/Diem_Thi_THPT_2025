import numpy as np
import pandas as pd

df = pd.read_csv("diem_toan_2025.csv")
sample = df.sample(n=100, random_state = 42)

sample.to_csv("sample_100.csv", index= False)