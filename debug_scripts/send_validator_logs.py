#!/bin/python3
import argparse
import boto3
import datetime
import io
import urllib.parse

parser = argparse.ArgumentParser(description='Send logs to near.')
parser.add_argument('--log_file', type=str, help='Absolute path to log file.', required=True)
parser.add_argument('--account', type=str, help='Near account id.', required=True)
parser.add_argument('--time_range', type=int, help='Filter logs for last x seconds.', required=True)
args = parser.parse_args()

log_file_path = args.log_file
end_timestamp = datetime.datetime.utcnow() #.strftime("%Y-%m-%dT%H:%M:%SZ")
start_timestamp = end_timestamp - datetime.timedelta(seconds=args.time_range)

min_start_timestamp = end_timestamp
max_end_timestamp = start_timestamp

# start_timestamp_str = start_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
# end_timestamp_str = end_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
print(f"Log time range: {start_timestamp} \t {end_timestamp}")

filtered_logs = io.StringIO()

# filter logs for time range
with open(log_file_path) as f:
    for line in f:
        split_lines = line.split("[0m", 1)[0].replace("\x1b[2m", "")
        dt = datetime.datetime.strptime(split_lines[:-5], "%b %d %H:%M:%S").replace(year=datetime.datetime.now().year)
        if dt >= start_timestamp and dt <= end_timestamp:
            filtered_logs.write(line)
            if dt < min_start_timestamp:
                min_start_timestamp = dt
            elif dt > max_end_timestamp:
                max_end_timestamp = dt

min_start_timestamp_str = min_start_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
max_end_timestamp_str = max_end_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")   
account = args.account
BUCKET = "near-protocol-validator-logs-public"

s3_destination = f"{account}/{min_start_timestamp_str}-{max_end_timestamp_str}.log"
s3 = boto3.resource('s3')
s3.Bucket(BUCKET).upload_fileobj(io.BytesIO(filtered_logs.getvalue().encode()), f"logs/{s3_destination}")
print(f"Log File was upload to S3: https://{BUCKET}.s3.amazonaws.com/logs/{urllib.parse.quote(s3_destination)}")
filtered_logs.close()
