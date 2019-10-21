import boto3
from botocore.exceptions import ClientError
import io
import pandas as pd
from io import StringIO
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import sys


DATE_SCRAMBLE_FLAG = True
DATE_FOMARTS = [
    {'TYPE': 'DT', 'FORMAT': '%Y%m%d%H%M%S'}
    , {'TYPE': 'DT', 'FORMAT': '%Y%m%d%I%M%S'}
    , {'TYPE': 'D', 'FORMAT': '%Y%m%d'}
    , {'TYPE': 'D', 'FORMAT': '%m-%d-%Y'}
    , {'TYPE': 'DT', 'FORMAT': '%m-%d-%Y %H:%M:%S'}
    , {'TYPE': 'DT', 'FORMAT': '%m-%d-%Y %I:%M:%S'}
    , {'TYPE': 'D', 'FORMAT': '%m/%d/%Y'}
    , {'TYPE': 'DT', 'FORMAT': '%m/%d/%Y %I:%M:%S'}
    , {'TYPE': 'DT', 'FORMAT': '%m/%d/%Y %H:%M:%S'}

    , {'TYPE': 'D', 'FORMAT': '%d-%b-%Y'}
    , {'TYPE': 'DT', 'FORMAT': '%d-%b-%Y %H:%M:%S'}
    , {'TYPE': 'DT', 'FORMAT': '%d-%b-%Y %I:%M:%S'}
    , {'TYPE': 'D', 'FORMAT': '%d-%b-%y'}
    , {'TYPE': 'DT', 'FORMAT': '%d-%b-%y %H:%M:%S'}
    , {'TYPE': 'DT', 'FORMAT': '%d-%b-%y %I:%M:%S'}

        , {'TYPE': 'D', 'FORMAT': '%d/%b/%Y'}
    , {'TYPE': 'DT', 'FORMAT': '%d/%b/%Y %H:%M:%S'}
    , {'TYPE': 'DT', 'FORMAT': '%d/%b/%Y %I:%M:%S'}
    , {'TYPE': 'D', 'FORMAT': '%d/%b/%y'}
    , {'TYPE': 'DT', 'FORMAT': '%d/%b/%y %H:%M:%S'}
    , {'TYPE': 'DT', 'FORMAT': '%d/%b/%y %I:%M:%S'}
]
DATE_PATTERNS = r'(^\d{8}$)|(^\d{14}$)|(^\d{2}[/-]\d{2}[/-]\d{4}\s\d{2}[:\s]\d{2}[:\s]\d{2}$)|(^\d{2}[/-][a-zA-Z]{3}[/-]\d{2,4})|(^\d{2}[/-][a-zA-Z]{3}[/-]\d{2,4}\s\d{2}[:\s]\d{2}[:\s]\d{2}$)'


def _sqs_polling(sqs_client, queue_url):
    try:
        print("Connecting to SQS to fetch file details.")
        sqsReadResponse = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, MessageAttributeNames=['All'], VisibilityTimeout=0, WaitTimeSeconds=0)
        print("File details are fetched from SQS.")
        return sqsReadResponse
    except Exception as e:
        print("Fatal error occured while trying to reading meassage from SQS. Error returned:" + str(e))
        return False


def _s3_read_operations(s3_client, s3_resource, bucket_name, key, region=None):
    try:
        pendingKey = key
        pendingObj = s3_resource.Object(bucket_name, pendingKey)
        processingKey = 'processing/' + key.split('/')[-1]

        print(f'Starting to copy {pendingKey} to {processingKey}.')
        s3_resource.meta.client.copy({'Bucket': bucket_name, 'Key': pendingKey}, bucket_name, processingKey)
        pendingObj.delete()
        print(f'{pendingKey} is copied to {processingKey}.')

        print(f'Starting to read {processingKey}.')
        df = pd.read_csv(io.BytesIO(s3_client.get_object(Bucket=bucket_name, Key=processingKey)['Body'].read()))
        print('Read operation completed.')
        return df
    except Exception as e:
        print("Fatal error occured while trying to reading data from S3. Error returned:" + str(e))
        return pd.DataFrame({})


def _date_scramble(val):
    try:
        for dt_format in DATE_FOMARTS:
            try:
                if dt_format.get('TYPE', 'D') == 'D':
                    date_val = datetime.strptime(val, dt_format.get('FORMAT', '%d-%m-%Y')).date()
                elif dt_format.get('TYPE', 'D') == 'DT':
                    date_val = datetime.strptime(val, dt_format.get('FORMAT', '%d-%m-%Y'))
                elif dt_format.get('TYPE', 'D') == 'T':
                    date_val = datetime.strptime(val, dt_format.get('FORMAT', '%d-%m-%Y')).time()
                else:
                    raise ValueError

                date_val += relativedelta(days=1)
                date_val += relativedelta(months=1)
                date_val += relativedelta(years=1)
                return datetime.strftime(date_val, dt_format.get('FORMAT', '%d-%m-%Y'))
            except ValueError:
                pass
            except Exception as e:
                print(f"Date conversion failed for {val}. Error returned: " + str(e))
                return False

        return True
    except Exception as e:
        print('Unexpected exception occurred. Returned Error: ' + str(e))
        sys.exit(1)


def _str_scramble(val):
    try:
        if val == 'nan':
            return 'NA'
        else:
            if DATE_SCRAMBLE_FLAG and re.match(DATE_PATTERNS, val):
                date_val = _date_scramble(val)
                if date_val:
                    return date_val

            lst_char = []
            for char in val:
                if char == '9':
                    lst_char.append('0')
                elif char == 'z':
                    lst_char.append('a')
                elif char == 'Z':
                    lst_char.append('A')
                else:
                    lst_char.append(chr(ord(char) + 1))
            return ''.join(lst_char)
    except Exception as e:
        print('Unexpected exception occurred. Returned Error: ' + str(e))
        sys.exit(1)


def _dec_scramble(val):
    try:
        if DATE_SCRAMBLE_FLAG and re.match(DATE_PATTERNS, val):
            date_val = _date_scramble(val)
            if date_val:
                return date_val
        lst_char = []
        if val[0] == '-' or val[0] == '+':
            char_cnt = -1
        else:
            char_cnt = 0

        for char in val:
            if char_cnt < 1:
                lst_char.append(char)
            elif char == '9':
                lst_char.append('0')
            elif char not in '012345678':
                lst_char.append(char)
            else:
                lst_char.append(chr(ord(char) + 1))
            char_cnt += 1
        return ''.join(lst_char)
    except Exception as e:
        print('Unexpected exception occurred. Returned Error: ' + str(e))
        sys.exit(1)


def _channel(val):
    try:
        if type(val) != str:
            str_val = str(val)
        else:
            str_val = val

        clean_val = str_val.strip('"')
        if re.match(r'^-?[0-9\.]*$', clean_val):
            scrambled_val = _dec_scramble(clean_val)
        else:
            scrambled_val = _str_scramble(clean_val)

        if str_val[0] == '"':
            return '"' + scrambled_val + '"'
        else:
            return scrambled_val
    except Exception as e:
        print('Unexpected exception occurred. Returned Error: ' + str(e))
        sys.exit(1)


def _scramble(src_df):
    try:
        tgt_df = src_df.copy()
        for col in src_df.columns:
            tgt_df[col] = src_df[col].apply(_channel)
        return tgt_df
    except Exception as e:
        print('Unexpected exception occurred. Returned Error: ' + str(e))
        return pd.DataFrame({})


def _data_scrambler(s3_client, s3_resource, bucket_name, key, df, region=None):
    try:
        print(f'Starting to scramble "{key}".')
        scrambledDf = _scramble(df)
        if scrambledDf.empty:
            return pd.DataFrame({})
        print(f'"{key}" is scrambled.')
        return scrambledDf
    except Exception as e:
        print("Fatal error occured while trying to scramble the input file data. Error returned:" + str(e))
        return pd.DataFrame({})


def _dynamodb_write_operation(dynamodb_client, dynamodb_resource, dynamodb_table, df, region=None):
    try:
        print('Extracting item count from Dynamo DB table.')
        #countResponse = dynamodb_client.describe_table(TableName=dynamodb_table)
        countResponse = dynamodb_client.scan(TableName=dynamodb_table)
        tableCount = countResponse['Count']
        print('Starting to write scrambled data to target table in Dynamo DB.')
        table = dynamodb_resource.Table(dynamodb_table)
        iterCount = 1
        for row in df.apply(lambda x: x.to_dict(), axis=1):
            row['SFPP_ID'] = tableCount + iterCount
            table.put_item(Item=row)
            iterCount += 1
        print('Scrambled data is written in target table in Dynamo DB.')
        return True
    except Exception as e:
        print("Fatal error occured while trying to write target table in Dynamo DB. Error returned:" + str(e))
        return False


def _s3_write_operations(s3_client, s3_resource, bucket_name, key, df, region=None):
    try:
        processingKey = 'processing/' + key.split('/')[-1]
        processingObj = s3_resource.Object(bucket_name, processingKey)
        outputKey = 'output/' + key.split('/')[-1]
        outputObj = s3_resource.Object(bucket_name, outputKey)
        processedKey = 'processed/' + key.split('/')[-1]

        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        print(f'Starting to write {outputKey}.')
        outputObj.put(Body=csv_buffer.getvalue())

        print(f'Starting to copy {processingKey} to {processedKey}.')
        s3_resource.meta.client.copy({'Bucket': bucket_name, 'Key': processingKey}, bucket_name, processedKey)
        processingObj.delete()

        print(f'"{processingKey}" is scrambled and stored in "{outputKey}".')
        return True
    except Exception as e:
        print("Fatal error occured while trying to write target file to S3 bucket. Error returned:" + str(e))
        return False


def _sqs_msg_del(sqs_client, sqs_queue_url, receiptHandle):
    try:
        print('Starting to delete message from SQS.')
        sqsDelResponse = sqs_client.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receiptHandle)
        time.sleep(5)
        print('Message is delete from SQS.')
        return sqsDelResponse
    except Exception as e:
        print("Fatal error occured while trying to delete used message from SQS. Error returned:" + str(e))
        return False


if __name__ == '__main__':
    try:
        print("Scrambler is starting:")
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/928765701029/s3fileprocessingpocsqs'
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        dynamodb_resource = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_table = 'TBL_S3_FILE_PROCESSING_POC'

        sqsReadResponse = _sqs_polling(sqs_client, sqs_queue_url)
        if len(sqsReadResponse.get('Messages', '')) <= 0:
            print("No files are available to process.")
        else:
            bucket = 's3fileprocessorpocbucket'
            sqsPayload = sqsReadResponse['Messages'][0]
            fileToBeProcessed = sqsPayload['Body']
            receiptHandler = sqsPayload['ReceiptHandle']
            sqsDelResponse = _sqs_msg_del(sqs_client, sqs_queue_url, receiptHandler)
            if not sqsDelResponse:
                raise Exception(f'SQS message clearance has failed.')

            processingKey = 'processing/' + fileToBeProcessed.split('/')[-1]
            processingObj = s3_resource.Object(bucket, processingKey)
            sourceDf = _s3_read_operations(s3_client, s3_resource, bucket, fileToBeProcessed)
            if sourceDf.empty:
                raise Exception(f'Source feed- {fileToBeProcessed} could not be read or it is an empty file.')
            scrambledDf = _data_scrambler(s3_client, s3_resource, 's3fileprocessorpocbucket', fileToBeProcessed, sourceDf)
            if scrambledDf.empty:
                raise Exception(f'Scrambling process has failed.')
            tableWriteStatus = _dynamodb_write_operation(dynamodb_client, dynamodb_resource, dynamodb_table, scrambledDf)
            if not tableWriteStatus:
                raise Exception(f'Data wrirring to Dynamo DB table has failed.')
            outputStatus = _s3_write_operations(s3_client, s3_resource, bucket, fileToBeProcessed, scrambledDf)
            if not outputStatus:
                raise Exception(f'Output data writting has failed.')
        print("Scrambler is going back to sleep.")
    except Exception as e:
        print("Fatal error occured while trying to process file from S3. Error returned:" + str(e))
        failedKey = 'failed/' + fileToBeProcessed.split('/')[-1]
        s3_resource.meta.client.copy({'Bucket': bucket, 'Key': processingKey}, bucket, failedKey)
        processingObj.delete()
