#!/bin/python3
import argparse
import boto3
import datetime
import gzip
import io
import sys
import urllib.parse


def filter_log_file(log_file: str, start_time: datetime.datetime,
                    end_time: datetime.datetime) -> list:
    """
    Filter log file for a time range.
    start_time: datetime.datetime
                    start time for logs
    end_time: datetime.datetime
                    end time for logs
    return: list
                list of log lines
    """
    print(f"Log time range: {start_time} \t {end_time}")

    filtered_logs = []

    # filter logs for time range
    with open(log_file) as f:
        for line in f:
            # [0m and [2m are ANSI shell color codes. Removing them to parse dates.
            split_lines = line.split("[0m", 1)[0].replace("\x1b[2m", "")
            dt = datetime.datetime.strptime(
                split_lines[:-5],
                "%b %d %H:%M:%S").replace(year=datetime.datetime.now().year)
            if start_time <= dt <= end_time:
                filtered_logs.append(line)
    return filtered_logs


def upload_to_s3(file_lines: list, account: str) -> str:
    """
    Upload File like object to S3 bucket near-protocol-validator-logs-public.
    file_obj: io.BytesIO
    account: str
    return string with S3 file path
    """
    BUCKET = "near-protocol-validator-logs-public"
    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    s3_destination = f"{account}/{current_time}.log.gzip"
    file_string = io.StringIO()
    for line in file_lines:
        file_string.write(line)

    file_obj = io.BytesIO(file_string.getvalue().encode())
    gzipped_content = gzip.compress(file_obj.read())
    print(
        f"uploading compressed file. File size is: {sys.getsizeof(gzipped_content)} Bytes"
    )

    s3 = boto3.resource('s3')
    s3.Bucket(BUCKET).upload_fileobj(io.BytesIO(gzipped_content),
                                     f"logs/{s3_destination}")
    s3_link = f"https://{BUCKET}.s3.amazonaws.com/logs/{urllib.parse.quote(s3_destination)}"
    print(f"Log File was uploaded to S3: {s3_link}")
    file_obj.close()
    return s3_link


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send logs to near.')
    parser.add_argument('--log_file',
                        type=str,
                        help='Absolute path to log file.',
                        required=True)
    parser.add_argument('--account',
                        type=str,
                        help='Near account id.',
                        required=True)
    parser.add_argument('--last_seconds',
                        type=int,
                        help='Filter logs for last x seconds.',
                        required=True)
    args = parser.parse_args()

    log_file_path = args.log_file
    end_timestamp = datetime.datetime.utcnow()
    start_timestamp = end_timestamp - datetime.timedelta(
        seconds=args.last_seconds)

    filtered_log_lines = filter_log_file(log_file=args.log_file,
                                         start_time=start_timestamp,
                                         end_time=end_timestamp)
    upload_to_s3(file_lines=filtered_log_lines, account=args.account)
