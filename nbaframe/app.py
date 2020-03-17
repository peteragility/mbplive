import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import json
import datetime
import decimal
import uuid

mediaLiveArn = 'arn:aws:medialive:us-west-2:072060221753:channel:3921754'
captureSecPerFrame = 2
#customLabelModelArn = 'arn:aws:rekognition:us-west-2:072060221753:project/nba-foul/version/nba-foul.2020-03-08T22.09.07/1583676547493'
customLabelModelArn = 'arn:aws:rekognition:us-west-2:072060221753:project/nba-foul/version/nba-foul.2020-03-17T03.03.27/1584385408116'
foulSecondsBeforeFreeThrow = 30
secondsBetweenFouls = 55

#dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
nbaLiveTable = dynamodb.Table('nbalive')
nbaFrameTable = dynamodb.Table('nbaframe')
nbaFoulTable = dynamodb.Table('nbafoul')

rekognition = boto3.client('rekognition', region_name='us-west-2')

mediapackage = boto3.client('mediapackage', region_name='us-west-2')

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def liveToFoulVod(liveStartTime, freeThrowTime):
    lastFoulTime = datetime.datetime.strptime('2019-01-01T09:19:19Z','%Y-%m-%dT%H:%M:%SZ')
    response = nbaFoulTable.query(
              Limit = 1,
              ScanIndexForward = False,
              KeyConditionExpression=Key('arn').eq(mediaLiveArn)
            )
    for item in response['Items']:
        lastFoulTime = datetime.datetime.strptime(item['startTime'],'%Y-%m-%dT%H:%M:%SZ')

    if lastFoulTime >= freeThrowTime - datetime.timedelta(seconds=secondsBetweenFouls):
        pass
    else:
        foulStart = max(freeThrowTime - datetime.timedelta(seconds=foulSecondsBeforeFreeThrow), liveStartTime)
        jobId = str(uuid.uuid4())

        foulStartHK = foulStart + datetime.timedelta(hours=8)
        foulId = 'foul-' + foulStartHK.strftime('%Y%m%d-%H%M%S')

        print('insert foul capture info into database')
        response = nbaFoulTable.put_item(
                    Item={
                    'arn': mediaLiveArn,
                    'startTime': foulStart.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'endTime': freeThrowTime.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'jobId': jobId,
                    'foulId': foulId
                    }
                )
        print (json.dumps(response))

        print('create live to vod harvest job...')
        response = mediapackage.create_harvest_job(
            StartTime=foulStart.strftime('%Y-%m-%dT%H:%M:%SZ'),
            EndTime=freeThrowTime.strftime('%Y-%m-%dT%H:%M:%SZ'),
            Id=jobId,
            OriginEndpointId='mbp-nba-channel',
            S3Destination={
                'BucketName': 'unicornflix-mbp-output-rdyo3ns5',
                'ManifestKey': 'fouls/' + foulId +'/index.m3u8',
                'RoleArn': 'arn:aws:iam::072060221753:role/mediapackage-harvest-role'
            }
        )
        print (json.dumps(response))


def lambda_handler(event, context):
    #print(json.dumps(event.get('detail')))

    statusCode = 200
    retData = {}

    for record in event['Records']:
        if record['eventName'] == 'ObjectCreated:Put':

            s3Event = record['s3']

            response = nbaLiveTable.query(
              Limit = 1,
              ScanIndexForward = False,
              KeyConditionExpression=Key('arn').eq(mediaLiveArn)
            )
            #print(json.dumps(response, indent=4, cls=DecimalEncoder))

            for item in response['Items']:
                print('retrieved latest medialive start time: ' + item.get('startTime'))
                liveStartTime = datetime.datetime.strptime(item.get('startTime'), '%Y-%m-%dT%H:%M:%SZ')

            retData['bucket'] = s3Event.get('bucket',{}).get('name')
            retData['fileKey'] = s3Event.get('object',{}).get('key')

            # use rekognition custom label to check if a frame is free throw related
            response = rekognition.detect_custom_labels(
                ProjectVersionArn=customLabelModelArn,
                Image={
                    'S3Object': {
                        'Bucket': retData['bucket'],
                        'Name': retData['fileKey']
                    }
                },
                MaxResults=2,
                MinConfidence=60
            )

            retData['freethrow'] = False
            for label in response['CustomLabels']:
                if label['Name'] == 'freethrow':
                    retData['freethrow'] = True


            if(retData['freethrow']):
                # Analyze fileKey suffix for timestamp
                retData['captureSeconds'] = int(retData['fileKey'].split('.')[1]) * captureSecPerFrame
                fileElapsedSeconds = retData['captureSeconds'] - 2

                captureTime = liveStartTime + datetime.timedelta(seconds=fileElapsedSeconds)

                print('free throw scene found! insert capture time to ddb')
                response = nbaFrameTable.put_item(
                    Item={
                    'frameId': retData['fileKey'],
                    'bucket': retData['bucket'],
                    'liveStartTime': liveStartTime.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'captureTime': captureTime.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'captureSeconds': retData['captureSeconds']
                    }
                )
                #print(json.dumps(response, indent=4, cls=DecimalEncoder))

                print('check and start live to vod harvest job if required...')
                liveToFoulVod(liveStartTime, captureTime)

            else:
                print('No free throw found.')

    
    print (json.dumps(retData))

    return {
        "statusCode": statusCode,
        "body": json.dumps(retData)
    }
