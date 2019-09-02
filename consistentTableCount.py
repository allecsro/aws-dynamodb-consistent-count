#!/usr/bin/python3

import boto3
import argparse
import progressbar
import time
import threading

# Declare the arguments required to perform the scan
parser = argparse.ArgumentParser()
parser.add_argument("-t", "--table", type=str, help="Provide the name of the table.",
                    nargs='?', required=True)
parser.add_argument("-p", "--profile", type=str, help="AWS credentials profile, the profile of the aws credentials as defined in ~/.aws/credentials",
                    nargs='?', default="default" , const=0)
parser.add_argument("-r", "--region", type=str, help="Provide an AWS Region (ex: eu-west-1).",
                    nargs='?', default="us-east-1" , const=0)
parser.add_argument("-e", "--endpoint", type=str, help="Optional. Provide an DynamoDB endpoint. (ex: https://dynamodb.us-east-1.amazonaws.com).",
                    nargs='?', default="https://dynamodb.us-east-1.amazonaws.com" , const=0)
parser.add_argument("-s", "--segments", type=int, help="Optional. Represents the total number of segments into which the Scan operation will be divided.",
                    nargs='?', default=1 , const=0)
parser.add_argument("-l", "--limit", type=int, help="Optional. The maximum number of items to be counted per request. This can help prevent situations where one worker consumes all of the provisioned throughput, at the expense of all other workers.",
                    nargs='?', default=500 , const=0)
parser.add_argument("-no", "--silent", help="If the script should output just the total count", action='store_true')
args = parser.parse_args()


# Initialize DynamoDB table
session = boto3.Session(profile_name=args.profile)
dynamodb = session.resource("dynamodb", region_name=args.region, endpoint_url=args.endpoint)
table = dynamodb.Table(args.table)

if args.silent == False:
    print(f'Checking if table {table} exists and or waiting to be created...')

# Wait if the table does not exist
table.meta.client.get_waiter('table_exists').wait(TableName=args.table)

if args.silent == False:
    print(f'Counting records for table {table} on region {args.region}...')
    print(f'Aprox item count: {table.item_count}')


# Define global variables
threadLock = threading.Lock()
totalCount = 0
threads = []

# Initialize the progress bar if the output is verbose
if args.silent == False:
    bar = progressbar.ProgressBar(maxval=table.item_count, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


def update_total_count(count):
    global totalCount
    global bar
    threadLock.acquire()

    totalCount += count

    if args.silent == False:
        # Update the progress bar, but keep in mind that we might get more items than the initial estimated items count
        bar.update(min(totalCount, table.item_count))

    threadLock.release()


class scanThread (threading.Thread):
    def __init__(self, segment, name):
      threading.Thread.__init__(self)
      self.segment = segment
      self.name = name
    def run(self):
        response = table.scan(
            Select="COUNT",
            TotalSegments=args.segments,
            Segment=self.segment,
            Limit=args.limit,
            ConsistentRead=True
        )

        update_total_count(response.get('Count', 0))

        while 'LastEvaluatedKey' in response: 
            lastEvaluatedKey = response.get('LastEvaluatedKey', None)
            
            response = table.scan(
                Select="COUNT",
                ConsistentRead=True,
                TotalSegments=args.segments,
                Segment=self.segment,
                Limit=args.limit,
                ExclusiveStartKey=lastEvaluatedKey
            )

            if (response['ResponseMetadata']['HTTPStatusCode'] != 200):
                if args.silent == False:
                    print(response)
                break
            
            update_total_count(response.get('Count', 0))  



# Record the start time of the script for statistics
start = time.time()

# Start the threads
for i in range(args.segments):
    thread = scanThread(i, f'ScanSegment-{i}')
    thread.start()
    threads.append(thread)


# Wait for all threads to complete
for t in threads:
   t.join()

if args.silent == False:
    bar.finish()
    print(f'Scanned item count: {totalCount}')
    print(f'Execution time {int(time.time() - start)} seconds')
else:
    print(totalCount)