import boto3
import datetime
from pytz import timezone
import pandas as pd
import awswrangler as wr

# Create CloudWatch client
cw = boto3.client('cloudwatch')

#List CloudWatch metrics
def list_metrics(table_name, metric_name):

    if table_name == 'all':
        list_metrics_response = cw.list_metrics(
                Namespace = 'AWS/DynamoDB',
                MetricName = metric_name
            )
        try:       
            list_metrics_response += cw.list_metrics(
                Namespace = 'AWS/DynamoDB',
                MetricName = metric_name ,
                NextToken = list_metrics_response['NextToken']
            )
            return list_metrics_response
        except:
            print ('Error listing metrics for ALL DynamoDB tables')
            # TODO Return null?
    else:
        list_metrics_response = cw.list_metrics(
                Namespace = 'AWS/DynamoDB',
                MetricName = metric_name,
                Dimensions = [
                    {
                        'Name': 'TableName',
                        'Value': table_name
                    },
                ]
            )
        try:       
            list_metrics_response += cw.list_metrics(
                Namespace = 'AWS/DynamoDB',
                MetricName = metric_name ,
                Dimensions = [
                    {
                        'Name': 'TableName',
                        'Value': table_name
                    },
                ],
                NextToken = list_metrics_response['NextToken']
            )
            return list_metrics_response
        except: 
            print ('Error listing metrics for DynamoDB table: ' + table_name)
            # TODO Return null?

# Get statistics for a given metric and table(s)
def get_metric_statistics(table_name,metric_name,interval,end_time,period,statistics):

    metr_list = []
    
    # Get list of metrics
    metrics = list_metrics(table_name,metric_name)

    if not metrics:
        for key,value in metrics.items():
            if key == 'Metrics':
                for index, val in enumerate(value):

                    # Each dimension represents either the base table or an index
                    print(index + ". Metrics for: ", val['Dimensions'])

                    # Get metric stats
                    get_metric_statistics_response = cw.get_metric_statistics(
                        Namespace = val['Namespace'],
                        MetricName = val['MetricName'],
                        StartTime = end_time - datetime.timedelta(days=interval),
                        EndTime = end_time,
                        Dimensions = val['Dimensions'],
                        Period = period,
                        Statistics = [statistics]
                    )
                    
                    try:
                        # If it's just the base table, val['Dimensions'] will only have one item
                        # If it's the index, then there will be two items in val['Dimensions'] 
                        #   - val['Dimensions'][0] = Table name
                        #   - val['Dimensions'][1] = Index name
                        
                        name = str(val['Dimensions'][0]['Value']) + ":" + str(val['Dimensions'][1]['Value'])
                    except: 
                        name = str(val['Dimensions'][0]['Value'])
                    
                    tmdf = pd.DataFrame.from_dict(get_metric_statistics_response['Datapoints'])
                    tmdf['name'] = name
                    tmdf['metric_name'] = val['MetricName']
                    metr_list.append(tmdf)

        print ('No metrics found. Please check if the table exists or if the time period selected has metrics available')
        # TODO: Return null?

    try:
        if not metr_list:
            # Add comments here. Explain what's happening below
            # TODO: Build data in the format below
            metrdf = pd.concat(metr_list,ignore_index=True)
            metrdf.columns = ['timestamp','unit','Stat','name','metric_name']
            del metrdf['Stat']
            metrdf['statistic'] = statistics
            metrdf = metrdf[['metric_name','statistic','timestamp','name','unit']]
            metrdf['timestamp'] = metrdf['timestamp'].astype('datetime64[ns]')
        else:
            print ('Metrics list empty')
            # TODO: Return null?
    except:
        print ('Exception thrown when building metrics dataframe')
        # TODO: Return null?
        metrdf = None

    return metrdf

#Publish metrics to S3
def publish_metrics_to_S3(params):

    athenadf = []
    crcu = 'ConsumedReadCapacityUnits'
    cwcu = 'ConsumedWriteCapacityUnits'
    pru = 'ProvisionedWriteCapacityUnits'
    pwu = 'ProvisionedReadCapacityUnits'
    pperiod = 3600
    cperiod = 60
    cstatistics = 'Sum'
    pstatistics = 'Average'
    tablename = params['dynamotablename']
    athenatablename = params['tablename']
    bucket = params['bucket']
    etime = params['endtime']
    interval = params['interval'] 
    etime = datetime.datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')
    actiontype = params['action']

    for i in range(interval):

        endtime = etime - datetime.timedelta(days=i) 

        #Get metric statistics for Provisioned Read Capacity Units
        prcudf = get_metric_statistics (tablename, pru, 1, endtime, pperiod, pstatistics)   

        #Get metric statistics for Provisioned Write Capacity Units    
        pwcudf = get_metric_statistics (tablename, pwu, 1, endtime, pperiod, pstatistics) 

        #Get metric statistics for Read Capacity Units
        crcudf = get_metric_statistics (tablename, crcu, 1, endtime, cperiod, cstatistics)

        #Get metric statistics for Write Capacity Units
        cwcudf = get_metric_statistics (tablename, cwcu, 1, endtime, cperiod, cstatistics)

        resdf = [cwcudf,crcudf,pwcudf,prcudf]
        resultdf = pd.concat(resdf,ignore_index=True)
        athenadf.append(resultdf)
            
    if actiontype == 'insert':  
        print("Appending to existing table")
        athenadf = pd.concat(athenadf,ignore_index=True)
        location = 's3://' + bucket + '/' + 'metrics' + '/ests/'

        # Write to S3 bucket in parquet format
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
        print("Overwrite existing table")
        athenadf = pd.concat(athenadf,ignore_index=True)
        location = 's3://' + bucket + '/' + 'metrics' + '/ests/'

        # Write to S3 bucket in parquet format
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