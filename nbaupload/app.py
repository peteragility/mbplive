import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import json
import datetime
import decimal
import uuid

#dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
vodMetaData = dynamodb.Table('vodAsset-4l56b2bntzeclhdwpwecy3o6rm-mbp')
vodObject = dynamodb.Table('videoObject-4l56b2bntzeclhdwpwecy3o6rm-mbp')

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def lambda_handler(event, context):
    #print(json.dumps(event.get('detail')))

    statusCode = 200
    retData = {}

    for record in event['Records']:
        if record['eventName'] == 'ObjectCreated:Put':

            s3Event = record['s3']
            bucketName = s3Event.get('bucket',{}).get('name')
            fileKey = s3Event.get('object',{}).get('key')

            if "index.m3u8" in fileKey:
                retData['bucket'] = bucketName
                retData['fileKey'] = fileKey
                retData['videoId'] = fileKey[fileKey.index('/')+1:fileKey.index("index.m3u8") - 1]

                print('insert video object...')
                response = vodObject.put_item(
                    Item={
                    '__typename': 'videoObject',
                    'createdAt': datetime.datetime.utcnow().isoformat(sep='T', timespec='milliseconds')+'Z',
                    'id': retData['videoId'],
                    'updatedAt': datetime.datetime.utcnow().isoformat(sep='T', timespec='milliseconds')+'Z'
                    }
                )

                print('insert video meta data...')
                response = vodMetaData.put_item(
                    Item={
                    '__typename': 'vodAsset',
                    'createdAt': datetime.datetime.utcnow().isoformat(sep='T', timespec='milliseconds')+'Z',
                    'description': retData['videoId'],
                    'title': retData['videoId'],
                    'id': str(uuid.uuid4()),
                    'updatedAt': datetime.datetime.utcnow().isoformat(sep='T', timespec='milliseconds')+'Z',
                    'vodAssetVideoId': retData['videoId']
                    }
                )
            else:
                continue

    
    print (json.dumps(retData))

    return {
        "statusCode": statusCode,
        "body": json.dumps(retData)
    }
