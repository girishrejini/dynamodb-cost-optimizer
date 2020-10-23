import boto3
import time 
import csv
import athenaquery
import os
import tablev2
import getmetrics

def lambda_handler(event, context):
    dynamotablename = event['dynamotablename']
    action = event['actiontype']
    params = {
        'action' : event['actiontype'],
        'dynamotablename' : event['dynamotablename'],
        'endtime' : event['endtime'],
        'tablename' : event['athenatablename'],
        'region' : os.environ.get("region"),
        'database': os.environ.get("database"),
        'bucket' : os.environ.get("bucket"),
        'prefix' : os.environ.get("prefix"),
        'period' : os.environ.get("period"),
        'utilization' : os.environ.get('utilization'),
        'min' : os.environ.get('minimum'),
        'interval' : event['interval']
    }
    
    #Publish CloudWatch metrics for DynamoDB tables to S3
    print ("Publishing CloudWatch metrics for DynamoDB tables to S3...")

    publish_metrics_response = getmetrics.gettingmetrics(params)
    
    print ("Publish metrics status: " + publish_metrics_response)

    #TODO: Refactor the following code after refactoring getmetrics file
    if publish_metrics_response != 'Success':
        raise NameError(status)
        return {
            'Message' : status ,
            'StatusCode' :  500
        }
    else:
        est_status = tablev2.estimate(params)
        if (est_status != 'Success') :
            raise NameError(cost2_status)
            return {
                'Message' : cost2_status ,
                'StatusCode' :  500
            }
        else:
            cost2_status = athenaquery.createdynamocostv2(params)
            if (cost2_status == 'FAILED') or (cost2_status == 'CANCELLED') or (cost2_status == 'RUNNING')or (cost2_status == False) or (cost2_status == 'QUEUED'):
                raise NameError(cost2_status)
                return {
                    'Message' : cost2_status ,
                    'StatusCode' :  500
                }
            else:
                model_status = athenaquery.createdynamomode(params)
                reserv_status = athenaquery.createreservedcost(params)
                if (model_status == 'FAILED') or (model_status == 'CANCELLED') or (model_status == 'RUNNING')or (model_status == False) or (model_status == 'QUEUED'):
                    raise NameError(model_status)
                    return {
                        'Message' : model_status ,
                        'StatusCode' :  500
                    }
                else :
                    querystatus = athenaquery.querydynamomode(params)
                    return {
                        'Queryrestul' : querystatus ,
                        'StatusCode' : 200
                    }