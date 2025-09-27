import pandas as pd
import glob
import os

folder_path = "data_2025"

# tránh merge lại file đã merge
all_files = [f for f in glob.glob(os.path.join(folder_path, "*.csv")) if "diem_thi_all" not in f]

df_list = [pd.read_csv(f) for f in all_files]
merged_df = pd.concat(df_list, ignore_index=True)

merged_df = merged_df.drop_duplicates(subset="SBD")

merged_df.to_csv(os.path.join(folder_path, "diem_thi_all.csv.gz"), index=False, compression="gzip")

# merged_df.to_csv(os.path.join(folder_path, "diem_thi_all.csv"), index=False)

