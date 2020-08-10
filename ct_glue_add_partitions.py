import time
import sys
import datetime

import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


s3 = boto3.resource('s3')
glue_client = boto3.client('glue', 'us-east-1')

args = getResolvedOptions(
    sys.argv, ["s3bucket", "s3path", "years", "initial-load"])

bucket = s3.Bucket(args['s3bucket'])
start_prefix = args['s3path']
days = []

if args['initial_load'] == 'y':
    years = args['years'].split(',')
    months = ['01', '02', '03', '04', '05',
              '06', '07', '08', '09', '10', '11', '12']
    print('initial load: ', bucket.name, "-", start_prefix,
          "-", years, "-", months, '-', days, '-', len(days))
else:
    now = datetime.datetime.now()
    years = [now.strftime("%Y")]
    months = [now.strftime("%m")]
    days = [str(now.day).zfill(2)]
    print('daily load: ', bucket.name, "-", start_prefix,
          "-", years, "-", months, '-', days, '-', len(days))

partitionValueList = []
LocationList = []
tabledef = {}
bucketList = {}


def get_path(s3_path, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cloudtrail_partitions')

    try:
        response = table.get_item(Key={'s3_path': s3_path})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        if 'Item' in response:
            return response['Item']
        else:
            return {'s3_path': 'not found'}


def put_path(s3_path, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cloudtrail_partitions')
    response = table.put_item(
        Item={
            's3_path': s3_path
        }
    )
    return response


def batch_put_path(s3_path, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cloudtrail_partitions')

    with table.batch_writer() as batch:
        for i in range(0, len(s3_path)):
            batch.put_item(Item={'s3_path': s3_path[i]})

    return


def get_glue_table_definition(glue_client):
    tableInput = {}

    table_def = glue_client.get_table(
        DatabaseName='cloudtrail_db',
        Name='cloudtrail_logs'
    )

    tableInput['DatabaseName'] = table_def['Table']['DatabaseName']
    tableInput['Name'] = table_def['Table']['Name']
    tableInput['Retention'] = table_def['Table']['Retention']
    tableInput['StorageDescriptor'] = table_def['Table']['StorageDescriptor']
    tableInput['StorageDescriptor']['InputFormat'] = 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
    tableInput['StorageDescriptor']['OutputFormat'] = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    tableInput['StorageDescriptor']['SerdeInfo'] = {
        'Name': 'CloudTrailSerde',
        'SerializationLibrary': 'com.amazon.emr.hive.serde.CloudTrailSerde',
        'Parameters': {
            'serialization.format': '1'
        }
    }
    tableInput['PartitionKeys'] = [
        {
            'Name': 'account',
            'Type': 'string'
        },
        {
            'Name': 'region',
            'Type': 'string'
        },
        {
            'Name': 'year',
            'Type': 'string'
        },
        {
            'Name': 'month',
            'Type': 'string'
        },
        {
            'Name': 'day',
            'Type': 'string'
        },
    ]
    tableInput['TableType'] = table_def['Table']['TableType']
    tableInput['Parameters'] = table_def['Table']['Parameters']

    return tableInput


def add_partitions(partitionValueList, tableInput, LocationList):
    partitionList = []
    j = 0

    print('in add_partitions, need to add ', str(
        len(partitionValueList)), ' partitions:')

    for i in range(0, len(partitionValueList)):
        partitionList.append({'Values': partitionValueList[i]})
        partitionList[j]['StorageDescriptor'] = tableInput['StorageDescriptor'].copy()
        partitionList[j]['StorageDescriptor']['Location'] = LocationList[i]
        if j == 99 or i == (len(partitionValueList)-1):
            print('submitting the batch create partitions for : ',
                  tableInput['DatabaseName'], tableInput['Name'])
            response = glue_client.batch_create_partition(
                DatabaseName=tableInput['DatabaseName'],
                TableName=tableInput['Name'],
                PartitionInputList=partitionList)
            partitionList = []
            j = 0
            print('Respose batch submitted')
        else:
            j = j+1

    return 'done'


for account_dict in bucket.meta.client.list_objects(Bucket=bucket.name, Prefix=start_prefix, Delimiter='/')['CommonPrefixes']:
    for region_dict in bucket.meta.client.list_objects(Bucket=bucket.name, Prefix=account_dict['Prefix']+'CloudTrail/', Delimiter='/')['CommonPrefixes']:
        for year in years:
            for month in months:
                month_prefix = (region_dict['Prefix']+year+'/'+month+'/')
                if len(days) == 0:
                    bucketList = bucket.meta.client.list_objects(
                        Bucket=bucket.name, Prefix=month_prefix, Delimiter='/')
                else:
                    bucketList = {'CommonPrefixes': [
                        {'Prefix': month_prefix+days[0]+'/'}]}
                if 'CommonPrefixes' in bucketList:
                    for day_dict in bucketList['CommonPrefixes']:
                        partition_path = 's3://' + \
                            bucket.name+'/'+day_dict['Prefix']
                        path = get_path(partition_path)
                        if path['s3_path'] == 'not found':
                            partitionValue = partition_path.split('/')[5:-1]
                            partitionValue.pop(1)
                            partitionValueList.append(partitionValue)
                            LocationList.append(partition_path)

if len(partitionValueList) > 0:
    if partitionValueList[len(partitionValueList)-1] == []:
        partitionValueList.pop(1)
    print('number of partition value lists', len(partitionValueList))
    print('number of s3 partitions discovered', len(LocationList))
    tabledef = get_glue_table_definition(glue_client)
    add_partitions(partitionValueList, tabledef, LocationList)
    batch_put_path(LocationList)
    msg = 'added '+str(len(partitionValueList))+' partions'
else:
    msg = 'no partitions to add'

print(msg)
