import os
import random
import boto3

s3 = boto3.resource("s3")

SAMPLE_SIZE = 1000
WET_PATHS = "wet.paths"

with open(WET_PATHS , "r") as f:
    wet_paths = f.readlines()
    random_paths = random.sample(WET_PATHS , SAMPLE_SIZE)
    paths_string = "".join(random_paths)
    s3.Object("wet-segments", "random-paths.txt").delete()
    s3.Object("wet-segments", "random-paths.txt").put(Body=paths_string)
    
