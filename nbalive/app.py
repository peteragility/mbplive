import boto3
from botocore.exceptions import ClientError
import json
import datetime
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
nbaLiveTable = dynamodb.Table('nbalive')

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

    liveState = event.get('detail', {}).get('state')

    if liveState == 'RUNNING':
        retData['state'] = liveState
        retData['eventId'] = event.get('id')
        #retData['startTime'] = datetime.datetime.strptime(event.get('time'), '%Y-%m-%dT%H:%M:%S%z')
        retData['startTime'] = event.get('time')
        retData['mediaLiveArn'] = event.get('detail',{}).get('channel_arn')

        print('start ddb insert, live stream start at: ' + retData['startTime'])

        # insert mediaLive startTime record into ddb nbalive table
        response = nbaLiveTable.put_item(
            Item={
            'arn': retData['mediaLiveArn'],
            'startTime': retData['startTime'],
            'eventId': retData['eventId']
            }
        )
        print(json.dumps(response, indent=4, cls=DecimalEncoder))

    else:
        retData['state'] = liveState

    print (json.dumps(retData))

    return {
        "statusCode": statusCode,
        "body": json.dumps(retData)
    }

# Usage: dot_get(mydict, 'some.deeply.nested.value', 'my default')
def dot_get(_dict, path, default=None):
  for key in path.split('.'):
    try:
      _dict = _dict[key]
    except KeyError:
      return default
  return _dict
