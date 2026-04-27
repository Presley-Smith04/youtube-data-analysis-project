# used to read/write data from S3 and Glue
import awswrangler as wr

# used for data manipulation and flattening JSON
import pandas as pd

# helps decode S3 object keys, handles spaces, and special characters
import urllib.parse

# used to access environment variables in Lambda
import os


# Environment variables
# avpid having to hardcode some values 
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # get bucket name and object key from the S3 event trigger
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # decode the file path
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8'
    )

    try:
        # read the JSON file from S3 into a DataFrame
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # flatten the nested "items" field into a table format
        df_step_1 = pd.json_normalize(df_raw['items'])

        # write the cleaned data back to S3 as a Parquet 
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,  # output location
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation  # append / overwrite
        )

        return wr_response

    except Exception as e:
        # log error for debugging in CloudWatch
        print(e)
        print(
            'Error getting object {} from bucket {}. '
            'Check that the file exists and region is correct.'
            .format(key, bucket)
        )
        raise e
