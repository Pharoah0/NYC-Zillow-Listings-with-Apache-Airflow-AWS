import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    target_bucket = 'zillow-data-project-transformed' # Our location for transformed data.
    target_file_name = object_key[:-5] # Grabs the raw filename without the '.json' moniker.
    print(target_file_name) # For view in CloudWatch Logs.
   
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    print(response) # For view in CloudWatch Logs.
    data = response['Body']
    print(data) # For view in CloudWatch Logs.
    data = response['Body'].read().decode('utf-8')
    print(data) # For view in CloudWatch Logs.
    data = json.loads(data)
    print(data) # For view in CloudWatch Logs.
    f = []
    for i in data["results"]:
        f.append(i)
    df = pd.DataFrame(f)
    # Select specific columns
    selected_columns = ["zpid", "streetaddress", "unit", "bedrooms", "bathrooms", "livingarea", "price", "rentzestimate", "zestimate", "hometype", "newconstructiontype", "city", "state", "zipcode", "country", "latitude", "longitude", "daysonzillow", "homestatus", "homestatusforhdp", "priceforhdp", "isfeatured", "isnonowneroccupied", "ispreforeclosureauction", "ispremierbuilder", "isshowcaselisting", "isunmappable", "iszillowowned", "openhouse", "taxassessedvalue", "lotareaunit", "lotareavalue", "currency"]
    df = df[selected_columns]
    print(df)
    
    # Convert DataFrame to Parquet format
    parquet_data = df.to_parquet(index=False)
    
    # Upload Parquet to S3
    bucket_name = target_bucket
    object_key = f"{target_file_name}.parquet"
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=parquet_data)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Parquet conversion and S3 upload completed successfully')
    }