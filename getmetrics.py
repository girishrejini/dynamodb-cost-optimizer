import boto3
import datetime
from pytz import timezone
import pandas as pd
import awswrangler as wr
cw = boto3.client('cloudwatch')
now = datetime.datetime.now()

#list metrics
def lstmetrics (tablename,metric_name):
    if tablename =='all':
        lsm =   cw.list_metrics(
            Namespace='AWS/DynamoDB',
            MetricName=metric_name
            )
        try:       
            lsm += cw.list_metrics(
            Namespace='AWS/DynamoDB',
            MetricName=metric_name ,
            NextToken = lsm['NextToken']
            )
        except: 
            return lsm
    else:
        lsm =   cw.list_metrics(
            Namespace='AWS/DynamoDB',
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': 'TableName',
                    'Value': tablename
                },
            ]
            )
        try:       
            lsm += cw.list_metrics(
            Namespace='AWS/DynamoDB',
            MetricName=metric_name ,
            Dimensions=[
                {
                    'Name': 'TableName',
                    'Value': tablename
                },
            ],
            NextToken = lsm['NextToken']
            )
        except: 
            return lsm
def getstatistcs (tablename,metric_name,interval,endtime,period,statistics) :
    index = 0
    print(endtime)
    metr_list = []
    if tablename == 'all':
        for key,value in lstmetrics(tablename,metric_name).items():
            if key == 'Metrics':
                for i in value:
                        print("fetching metrics for: " , value[index]['Dimensions'])
                        data = cw.get_metric_statistics(
                        Namespace=value[index]['Namespace'],
                        MetricName=value[index]['MetricName'],
                        StartTime=endtime - datetime.timedelta(days=interval),
                        EndTime=endtime,
                        Dimensions=value[index]['Dimensions'],
                        Period=period,
                        Statistics=[statistics]
                    
                        )
                        
                        try:
                            name = str(value[index]['Dimensions'][0]['Value']) + ":" + str(value[index]['Dimensions'][1]['Value'])
                        except: 
                            name = str(value[index]['Dimensions'][0]['Value'])
                        
                        tmdf = pd.DataFrame.from_dict(data['Datapoints'])
                        tmdf['name'] =  name
                        tmdf['metric_name'] = value[index]['MetricName']
                        metr_list.append(tmdf)
                        index += 1
                        statistics = statistics
    else:
        for key,value in lstmetrics(tablename,metric_name).items():
            if key == 'Metrics':
                for i in value:
                        print("fetching metrics for: " , value[index]['Dimensions'])
                        data = cw.get_metric_statistics(
                        Namespace=value[index]['Namespace'],
                        MetricName=value[index]['MetricName'],
                        StartTime=endtime - datetime.timedelta(days=interval),
                        EndTime=endtime,
                        Dimensions=value[index]['Dimensions'],
                        Period=period,
                        Statistics=[statistics]
                    
                        )
                        
                        try:
                            name = str(value[index]['Dimensions'][0]['Value']) + ":" + str(value[index]['Dimensions'][1]['Value'])
                        except: 
                            name = str(value[index]['Dimensions'][0]['Value'])
                        
                        tmdf = pd.DataFrame.from_dict(data['Datapoints'])
                        tmdf['name'] =  name
                        tmdf['metric_name'] = value[index]['MetricName']
                        metr_list.append(tmdf)
                        index += 1
                        statistics = statistics
    try:
        metrdf = pd.concat(metr_list,ignore_index=True)
        metrdf.columns = ['timestamp','unit','Stat','name','metric_name']
        del metrdf['Stat']
        metrdf['statistic'] = statistics
        metrdf = metrdf[['metric_name','statistic','timestamp','name','unit']]
        metrdf['timestamp'] = metrdf['timestamp'].astype('datetime64[ns]')
        return metrdf
    except:
        metrdf = None
        return metrdf
    
           
                



#Getting  Metrics
def gettingmetrics (params):
    athenadf = []
    rcu = 'ConsumedReadCapacityUnits'
    wcu = 'ConsumedWriteCapacityUnits'
    pru = 'ProvisionedWriteCapacityUnits'
    pwu = 'ProvisionedReadCapacityUnits'
    pperiod = 3600
    cperiod = 60
    cstatistics = 'Sum'
    pstatistics = 'Average'
    tablename = params['dynamotablename']
    athenatablename = params['tablename']
    bucket=params['bucket']
    etime = params['endtime']
    interval = params['interval'] 
    etime = datetime.datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')
    intr = interval - 1
    actiontype = params['action']
    for i in range(interval):
        endtime = etime - datetime.timedelta(days=i) 
        prudf = getstatistcs (tablename,pru,1,endtime,pperiod,pstatistics)       
        pwudf = getstatistcs (tablename,pwu,1,endtime,pperiod,pstatistics) 
        rcudf = getstatistcs (tablename,rcu,1,endtime,cperiod,cstatistics)
        wcudf = getstatistcs (tablename,wcu,1,endtime,cperiod,cstatistics)
        resdf = [wcudf,rcudf,pwudf,prudf]
        resultdf = pd.concat(resdf,ignore_index=True)
        athenadf.append(resultdf)
            
    if actiontype == 'insert'   :  
        print("appending to existing table")
        athenadf = pd.concat(athenadf,ignore_index=True)
        location = 's3://' + bucket + '/' + 'metrics' + '/ests/'

        wr.s3.to_parquet(
                df=athenadf,
                path=location,
                database='default',
                dataset=True,
                mode="append",
                table=athenatablename
            )
        #Create Athena/Glue Table from Parquet
        wr.catalog.table(database='default', table=athenatablename)
        return 'Success'
    else:
        print("overwriting to existing table")
        athenadf = pd.concat(athenadf,ignore_index=True)
        location = 's3://' + bucket + '/' + 'metrics' + '/ests/'

        wr.s3.to_parquet(
                df=athenadf,
                path=location,
                database='default',
                dataset=True,
                mode="overwrite",
                table=athenatablename
            )
        #Create Athena/Glue Table from Parquet
        wr.catalog.table(database='default', table=athenatablename)
        return 'Success'