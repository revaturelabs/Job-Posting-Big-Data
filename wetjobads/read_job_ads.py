import os
import glob

# TODO read job ads from S3 instead of locally
input_files = glob.glob("jobAds/*.csv")
out_path = "dat/100k_jobADs/"

out_files = glob.glob(out_path + "*")
for f in out_files:
    os.remove(f)

for filename in input_files:
  count = 1 
  with open(filename, "r") as inputf:
      for line in inputf.readlines():
        with open(out_path + str(count) + "_2021JAN_jobAD.txt", "w") as outputf:
          outputf.write(line)
          count += 1


   