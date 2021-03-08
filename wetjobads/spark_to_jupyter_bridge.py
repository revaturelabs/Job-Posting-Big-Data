import os
import glob

# TODO read job ads from S3 instead of locally
input_files = glob.glob("jobAds/*.csv")
output_path = "dat/100k_jobADs/"

for filename in input_files:
  i = 1 
  with open(filename, "r") as inputf:
      for line in inputf.readlines():
        with open(output_path + str(i) + "_2021JAN_jobAD.txt", "w") as outputf:
          outputf.write(line)
          i += 1


   