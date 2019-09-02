from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import argparse
import progressbar
from boto3.dynamodb.conditions import Key, Attr

# Declare the arguments required to perform the scan
parser = argparse.ArgumentParser()
parser.add_argument("-t", "--table", type=str, help="Provide the name of the table",
                    nargs='?', required=True)
parser.add_argument("-p", "--profile", type=str, help="AWS credentials profile, the profile of the aws credentials as defined in ~/.aws/credentials",
                    nargs='?', default="default" , const=0)
parser.add_argument("-r", "--region", type=str, help="Provide an AWS Region (ex: eu-west-1)",
                    nargs='?', default="us-east-1" , const=0)
parser.add_argument("-e", "--endpoint", type=str, help="Optiona. Provide an DynamoDB endpoint. (ex: https://dynamodb.us-east-1.amazonaws.com)",
                    nargs='?', default="https://dynamodb.us-east-1.amazonaws.com" , const=0)
args = parser.parse_args()


# Initialize DynamoDB table
session = boto3.Session(profile_name=args.profile)
dynamodb = session.resource("dynamodb", region_name=args.region, endpoint_url=args.endpoint)
table = dynamodb.Table(args.table)

print("Checking if table %s exists and if not waiting to be created..." % (table))

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName=args.table)

print("Counting records for table %s on region %s..." % (table, args.region))
print("Aprox item count: %d" % (table.item_count))


# Initialize the progress bar
bar = progressbar.ProgressBar(maxval=table.item_count, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
bar.start()


response = table.scan(
    Select="COUNT",
    ConsistentRead=True
)

totalCount = response.get('Count', 0)

while 'LastEvaluatedKey' in response: 
    lastEvaluatedKey = response.get('LastEvaluatedKey', None)
    
    response = table.scan(
        Select="COUNT",
        ConsistentRead=True,
        ExclusiveStartKey=lastEvaluatedKey
    )

    if (response['ResponseMetadata']['HTTPStatusCode'] != 200):
        print(response)
        break

    totalCount += response.get('Count', 0)

    # Update the progress bar, but keep in mind that we might get more items than the initial estimated items count
    bar.update(min(totalCount, table.item_count))

bar.finish()
print("Scanned item count: %d" % (totalCount))