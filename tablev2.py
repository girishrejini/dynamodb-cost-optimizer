import csv
import operator
import datetime
import pandas as pd
import awswrangler as wr


# print(df)


def maxA(i, j):
    if i > j:
        return i
    else:
        return j


def minA(i, j):
    if i > j:
        return j
    else:
        return i


def decrease60(L):
    return any(x > y for x, y in zip(L, L[1:]))


def estimateUnits(cu, utilization):
    finalcu = []
    prev = cu[0]
    finalcu += [prev]
    prev[6] = (prev[5] / utilization) * 100
    for i in range(1, len(cu)):

        current = cu[i]
        # compare with prev val
        if i <= 2:
            prev = current
            # adding 1 - 2 records to final list
            finalcu += [current]
            continue
        # creating a list with last 2 records.
        last2 = [v[5] for v in list(cu[i - 2: i])]
        # print(last2)
        last2max = max(last2)
        last2min = min(last2)
        maxV = maxA((last2min / utilization) * 100, prev[6])
        # scale out based on last 2 min Units.
        if maxV == (last2min / utilization) * 100:

            current[6] = (last2max / utilization) * 100
        else:
            current[6] = maxV

        # print(i,current)

        if i <= 14:
            prev = current
            # print(i, current)
            finalcu += [current]
            continue
        # Create list from last 15 Consumed Units
        last15 = [v[5] for v in list(cu[i - 15: i])]
        # print(last15)
        last15Max = max(last15)

        """
        if i > 32:
            print(last15)
            print(last15Max)
            print(minA(last15Max*1.3, current[6]))
        """

        if i >= 60:
            # Create list from last 60 Consumed Units
            last60 = [v[6] for v in list(cu[i - 60: i])]
            # if Table has not scale in in past 60 minutes then scale in
            if not decrease60(last60):
                current[6] = minA((last15Max / utilization) * 100, current[6])

        prev = current
        # adding current row to the result list
        finalcu += [current]
    return finalcu


def estimate(params):
    tablename = params['tablename']
    region = params['region']
    database = params['database']
    bucket = params['bucket']
    path = params['prefix']
    workgroup = 'AmazonAthenaPreviewFunctionality'
    period = int(params['period'])
    utilization = int(params['utilization'])
    location = 's3://' + params['bucket'] + '/' + params['prefix'] + '/est/'
    s3_output = 's3://' + params['bucket'] + '/' + params['prefix'] + '/temp/'
    esttable = tablename + 'est'

    intialqu = """SELECT * FROM "default"."%s";"""
    tablequ = intialqu % (tablename)
    finalwcu = []
    finalrcu = []
    name = []
    f = []
    # Query Athena to fetch Dynamodb metrics as Dataframe
    df = wr.athena.read_sql_query(
        sql=tablequ,
        s3_output=s3_output,
        database='default',
        workgroup=workgroup
    )
    # change dataframe to list.
    listdf = df.values.tolist()

    for row in listdf:
        row[4] = int(float(row[4]))
        # calculate Unites from minutes to seconds.
        row.append(round(row[4] / period))
        row.append(5)
        f += [row]
        # create a list with DynamoDB table names
        if row[3] not in name:
            name.append(row[3])

    # estimating Unites for ConsumedReadCapacityUnits

    for x in range(len(name)):
        # print(len(name))
        rcu = []
        for row in f:

            if (row[0] == 'ConsumedReadCapacityUnits') and (row[3] == name[x]):
                rcu += [row]

        rcu = sorted(rcu, key=lambda row: row[2], reverse=False)
        # estimate Unites
        finalrcu += estimateUnits(rcu, utilization)

    rcu_df = pd.DataFrame(finalrcu)
    rcu_df.columns = ['metric_name', 'statistic', 'timestamp',
                      'name', 'unit', 'unitps', 'estunit']

    # estimating Unites for ConsumedWriteCapacityUnits
    for x in range(len(name)):

        wcu = []
        for row in f:

            if (row[0] == 'ConsumedWriteCapacityUnits') and (row[3] == name[x]):
                wcu += [row]

        wcu = sorted(wcu, key=lambda row: row[2], reverse=False)
        # estimate Unites
        # adding current row to the result list
        finalwcu += estimateUnits(wcu, utilization)
    # adding the WCU to Dataframe
    wcu_df = pd.DataFrame(finalwcu)
    # set DataFrame column name
    wcu_df.columns = ['metric_name', 'statistic', 'timestamp',
                      'name', 'unit', 'unitps', 'estunit']
    # merge RCU and WCU into final DataFrame
    finaldf = [wcu_df, rcu_df]
    result = pd.concat(finaldf)
    # upload the DataFrame as parquet to S3
    wr.s3.to_parquet(
        df=result,
        path=location,
        database=database,
        dataset=True,
        mode="overwrite",
        dtype={'metric_name': 'string', 'statistic': 'string', 'timestamp': 'timestamp', 'name': 'string',
               'value': 'bigint', 'unit': 'double', 'estunit': 'double'},
        table=esttable
    )
    # Create Athena/Glue Table from Parquet
    wr.catalog.table(database=database, table=esttable)
    return "Success"