import boto3
import time 
import csv
import athena
import os

    
def createreservedcost(params):
    tablename=params['tablename']
    region=params['region']
    database=params['database']
    bucket=params['bucket']
    path=params['prefix']
  

    intialqu = """CREATE OR REPLACE VIEW %sresrved AS 
        SELECT
        "c"."metric_name" "metric_name"
        , "p"."max" "estmax"
        , "p"."min" "estmin"
        , "p"."avg" "estavg"
        , "c"."max" "max"
        , "c"."min" "min"
        , "c"."avg" "avg"
        , "p"."reservedcost" "estreservedcost"
        , "p"."normalcost" "estnormalcost"
        , "c"."reservedcost" "reservedcost"
        , "c"."normalcost" "normalcost"
        , "p"."totalreservedcost" "totalestreservedcost"
        , "p"."totalnormalcost" "totalestnormalcost"
        , "c"."totalreservedcost" "totalreservedcost"
        , "c"."totalnormalcost" "totalnormalcost"
        FROM
        ((
        SELECT
            "max"("provisionedunit") "max"
        , "min"("provisionedunit") "min"
        , "avg"("provisionedunit") "avg"
        , "Provisionedmetric_name"
        , (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 5.9E-5) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 2.99E-4) ELSE 0 END) "reservedcost"
        , (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 1.3E-4) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 6.5E-4) ELSE 0 END) "normalcost",
            (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 5.9E-5) * count(timestamp) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 2.99E-4) * count(timestamp) ELSE 0 END) "totalreservedcost"
        , (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 1.3E-4) * count(timestamp) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 6.5E-4) * count(timestamp) ELSE 0 END) "totalnormalcost"
        FROM
            (
            SELECT
                "Provisionedmetric_name"
            , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
            , "sum"("provisionedunit") "provisionedunit"
            FROM
                "default"."%scostv2"
            where ("ondemandcost") > ("provisionedcost")
            GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "Provisionedmetric_name"
        ) 
        GROUP BY "Provisionedmetric_name"
        )  p
        right JOIN (
        SELECT
            "max"("unit") "max"
        , "min"("unit") "min"
        , "avg"("unit") "avg"
        , "metric_name"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 5.9E-5) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 2.99E-4) ELSE 0 END) "reservedcost"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 6.5E-4) ELSE 0 END) "normalcost"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 5.9E-5)  * count(timestamp) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 2.99E-4) * count(timestamp) ELSE 0 END) "totalreservedcost"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 1.3E-4) * count(timestamp) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 6.5E-4) * count(timestamp) ELSE 0 END) "totalnormalcost"
        FROM
            (
            SELECT
                "metric_name"
            , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
            , "sum"("unit") "Unit"
            FROM
                "default"."%s"
            WHERE ("metric_name" IN ('ProvisionedWriteCapacityUnits', 'ProvisionedReadCapacityUnits'))
            GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "metric_name"
        ) 
        GROUP BY "metric_name"
        )  c ON ("p"."provisionedmetric_name" = "c"."metric_name"))"""
    resrvqu = intialqu % (tablename,tablename,tablename)
    params = {
        'region': region,
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': resrvqu
    }

    session = boto3.Session()

    ## Fucntion for obtaining query results and location 
    status = athena.query_results(session, params)
    #print("Locations: ",location)
    #print("Result Data: ")
    #print(restult_data)
    ## Function for cleaning up the query results to avoid redundant data
    return status
        
def createdynamocostv2(params):
    
    dynamotablename=params['dynamotablename']
    tablename=params['tablename']
    region=params['region']
    database=params['database']
    bucket=params['bucket']
    path=params['prefix']
    period=params['period']
    utilization=params['utilization']
    mins=params['min']
    
    intialqu = """CREATE OR REPLACE VIEW %scostv2 AS 
                SELECT
                *
                , split_part("name",':',1) as basetable
                , (CASE WHEN ("mode" = 'Ondemand') THEN "ondemandcost" ELSE null END) "ondemandactualcost"
                , (CASE WHEN ("mode" = 'Provisioned') THEN "provisionedcost" ELSE null END) "provisionedactualcost"
                FROM
                (
                SELECT
                    "c"."name"
                , "c"."timestamp"
                , "c"."metric_name"
                , "c"."cost" "ondemandcost"
                , "p"."estcost" "provisionedcost"
                , "p"."metric_name" "Provisionedmetric_name"
                , "p"."mode"
                , "c"."unit" "ondemandunit"
                , "p"."estunit" "provisionedunit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN null ELSE null END) "mode"
                    , "unit"
                    , "estunit"
                    
                    /*Dynamo Cost calc */
                    FROM
                        (   
                        SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sest"
                    WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                )  p
                LEFT JOIN default.%s ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                ) 
                        )
                    /*Dynamo Cost calc end */
                    WHERE ("metric_name" IN ('ConsumedWriteCapacityUnits'))
                )  c
                LEFT JOIN (
                    SELECT
                        "name"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN 'Ondemand' ELSE 'Provisioned' END) "mode"
                    , "unit"
                    , "estunit"
                    /*Dynamo Cost calc */
                    from (
                    SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sest"
                    WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                )  p
                LEFT JOIN "%s" ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                ) 
                    )    
                    /*Dynamo Cost calc end */
                    
                    WHERE ("metric_name" IN ('ProvisionedWriteCapacityUnits'))
                )  p ON (("c"."timestamp" = "p"."timestamp") AND ("c"."name" = "p"."name")))
                    
                UNION    SELECT
                    "c"."name"
                , "c"."timestamp"
                , "c"."metric_name"
                , "c"."cost"
                , "p"."estcost"
                , "p"."metric_name" "estmetric_name"
                , "p"."mode"
                , "c"."unit"
                , "p"."estunit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN null ELSE null END) "mode"
                    , "unit"
                    , "estunit"
                    /*Dynamo Cost calc */
                    from (
                    SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sest"
                    WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                )  p
                LEFT JOIN "%s" ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                ) 
                    )
                    /*Dynamo Cost calc end */
                    WHERE ("metric_name" IN ('ConsumedReadCapacityUnits'))
                )  c
                LEFT JOIN (
                    SELECT
                        "name"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN 'Ondemand' ELSE 'Provisioned' END) "mode"
                    , "unit"
                    , "estunit"
                    /*Dynamo Cost calc */
                    from (
                    SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sest"
                    WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                )  p
                LEFT JOIN "%s" ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name"
                ) 
                    )
                    /*Dynamo Cost calc end */
                    WHERE ("metric_name" IN ('ProvisionedReadCapacityUnits'))
                )  p ON (("c"."timestamp" = "p"."timestamp") AND ("c"."name" = "p"."name")))
                )"""
    costqu = intialqu % (tablename,tablename,tablename,tablename,mins,mins,tablename,tablename,tablename,tablename,
                        tablename,tablename,tablename,tablename,tablename,mins,mins,
                        tablename,tablename,tablename,tablename,tablename,tablename,tablename,
                        tablename,tablename,mins,mins,tablename,tablename,tablename,tablename,tablename,tablename,
                        tablename,tablename,tablename,mins,mins,tablename,tablename,tablename,tablename,tablename,tablename)
    params = {
        'region': region,
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costqu
    }


    session = boto3.Session()
    ## Fucntion for obtaining query results and location 
    status = athena.query_results(session, params)
    #print("Locations: ",location)
    #print("Result Data: ")
    #print(restult_data)
    ## Function for cleaning up the query results to avoid redundant data
    return status

def createdynamomode(params):
    action=params['action']
    dynamotablename=params['dynamotablename']
    endtime=params['endtime']
    tablename=params['tablename']
    region=params['region']
    database=params['database']
    bucket=params['bucket']
    path=params['prefix']
    period=params['period']


    intialqu = """CREATE OR REPLACE VIEW %smode AS 
            SELECT
            "name"
            , "basetable"
            , "provisionedcost"
            , "ondemandcost"
            , "currentmode"
            , "purposedmode"
            , "diff"
            , (CASE WHEN ("purposedmode" = 'Provisioned') THEN "provisionedcost" WHEN ("purposedmode" = 'Ondemand') THEN "ondemandcost" ELSE 0 END) "estcost"
            , (CASE WHEN ("currentmode" = 'Provisioned') THEN "provisionedcost" WHEN ("currentmode" = 'Ondemand') THEN "ondemandcost" ELSE 0 END) "currentcost"
            FROM
            (
            /* set difference in cost saving if change table throughput mode */
            SELECT
            *
            , (CASE WHEN (("purposedmode" <> "currentmode") AND ("purposedmode" = 'Provisioned')) THEN ((100 * ("provisionedcost" - "Ondemandcost")) / "Ondemandcost") WHEN (("purposedmode" <> "currentmode") AND ("purposedmode" = 'Ondemand')) THEN ((100 * ("Ondemandcost" - "provisionedcost")) / "provisionedcost") ELSE null END) "diff"
            FROM
            (
            SELECT
                "name"
            , "basetable"
            , "sum"("provisionedcost") "provisionedcost"
            , "sum"("ondemandcost") "Ondemandcost"
            , "mode" "currentmode"
            /* compare ondemand cost vs provision and suggest throuput mode */
            , (CASE WHEN ("sum"("ondemandcost") < "sum"("provisionedcost")) THEN 'Ondemand' WHEN ("sum"("ondemandcost") > "sum"("provisionedcost")) THEN 'Provisioned' ELSE null END) "purposedmode"
            FROM
                %scostv2
            GROUP BY "name", "mode","basetable"
            ))"""
    costmodequ = intialqu % (tablename,tablename)
    params = {
        'region': region,
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costmodequ
    }


    session = boto3.Session()
    ## Fucntion for obtaining query results and location 
    status = athena.query_results(session, params)
    #print("Locations: ",location)
    #print("Result Data: ")
    #print(restult_data)
    ## Function for cleaning up the query results to avoid redundant data
    return (status)
 
def querydynamomode(params):
    action=params['action']
    dynamotablename=params['dynamotablename']
    endtime=params['endtime']
    tablename=params['tablename']
    region=params['region']
    database=params['database']
    bucket=params['bucket']
    path=params['prefix']
    period=params['period']
   

    intialqu = """SELECT * FROM "default"."%smode";"""
    costmodequ = intialqu % (tablename)
    params = {
        'region': region,
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costmodequ
    }


    session = boto3.Session()
    ## Fucntion for obtaining query results and location 
    location, restult_data = athena.query_results(session, params)
    print("Locations: ",location)
    #print("Result Data: ")
    #restult_data = params('qu')
    return(location)
    ## Function for cleaning up the query results to avoid redundant data
