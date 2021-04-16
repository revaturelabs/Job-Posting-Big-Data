import boto3
s3 = boto3.resource("s3")

def delete(bucket_name, obj):
    s3.Object(bucket_name, obj).delete()
