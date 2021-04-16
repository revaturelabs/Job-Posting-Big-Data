import wget
import os
import json
import subprocess
import datetime
import boto3


def download_wetpaths(crawl):
    if not os.path.isfile("wet.paths"):
        url = f"https://commoncrawl.s3.amazonaws.com/crawl-data/{crawl}/wet.paths.gz"
        wget.download(url)
        subprocess.run(["gunzip", "wet.paths"])


def parse_config():
    with open("config.json") as f:
        return json.load(f)


def main():
    s3 = boto3.resource("s3")
    config = parse_config()
    download_wetpaths(crawl=config["crawl"])

    # read batch_size
    BATCH_SIZE = config["batch_size"]
    INPUT_BUCKET = config["input_bucket"]
    line_start = None
    line_end = None

    # read last line number read
    with open("batch_history.json") as f:
        logs = f.readlines()
        logs = [json.loads(line) for line in logs]
        last_read = logs[-1]["line_end"]

    # read appropriate lines and upload to S3
    with open("wet.paths") as f_in:
        with open("wet_batch.paths", "w") as f_out:
            line_start = last_read
            line_end = last_read + BATCH_SIZE + 1
            batch = f_in.readlines()[line_start:line_end]
            f_out.write("".join(batch))

    # upload to s3
    # aws s3 cp s3://wet-segments-7913/wet_batch.paths . && cat wet_batch.paths
    s3.Object(INPUT_BUCKET, "wet_batch.paths").delete()
    s3.meta.client.upload_file("wet_batch.paths", INPUT_BUCKET, "wet_batch.paths")

    # add new line to log file
    with open("batch_history.json", "a") as f:
        now = datetime.datetime.now().isoformat()
        log = {"loaded_at": now, "line_start": line_start, "line_end": line_end - 1}
        f.write("\n" + json.dumps(log))


main()