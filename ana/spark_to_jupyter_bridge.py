import os
import boto3

S3_BUCKET = 'emr-output-revusf'
JOBADS_KEY = 'jobads/part-00000-7c4ab6f5-bea0-4a82-b6a8-66a4f8820a74-c000.csv' 
OUTPUT_PATH = 'dat/100k_jobADs/' 

JOBADS_TMP = 'jobads.tmp'

def download_jobads():
  s3 = boto3.resource('s3')
  s3.Bucket(S3_BUCKET).download_file(JOBADS_KEY, JOBADS_TMP)
  print("Downloading job ads. This may take a few minutes...")

def extract_jobads():
  i = 0
  with open(JOBADS_TMP, 'r') as inputf:
      for line in inputf.readlines():
        with open(output_path + str(i) + "_2021JAN_jobAD.txt", "w") as outputf:
          outputf.write(line)
          i += 1

def cleanup():
  if os.path.exists(JOBADS_TMP):
    os.remove(JOBADS_TMP)

if __name__ == '__main__':
  # download_jobads()
  # extract_jobads()
  cleanup()



   