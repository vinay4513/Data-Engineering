# Databricks notebook source
# MAGIC %run "/Users/vinay.gadade@dctinc.com/Project/SAM_Table"

# COMMAND ----------

import pandas as pd

# COMMAND ----------

spark.conf.set("spark.hadoop.fs.s3a.access.key", "ASIAUR75QRCKIWBRPSLU")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "11kw0aRMjvE9DN0izH/CyyL21TlzT7x3YaDQ/0RL")

# COMMAND ----------

import boto3
import pandas as pd
from io import StringIO

# Convert PySpark DataFrame to pandas DataFrame
pandas_df = output_table.toPandas()

# # Convert DataFrame to CSV string
csv_buffer = StringIO()
pandas_df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)



# Configure AWS credentials and region
aws_access_key_id="ASIAUR75QRCKJZFNLZE3"
aws_secret_access_key="tXw91RrWlxY+JU39C4hfSaJH7arC5SnrrppDoN30"
aws_session_token="FwoGZXIvYXdzEDcaDEL6AvoIHgMct5lzxCLIAdAP5QImT0GzgXmNnNPStw+CAJu67b/0XUs/xxhBCyq2360fjyhbthdNtkaObh3IqB1o8TBbMFR9IFIQ4BP+u4YmCZ1aK/JhGZZhJILd0E24m5gn2AcoRXYpIHJwCkHRNWCkjZU2J8HEZjtwj01+LlAV4wnK1p1tx/SXlgx263hJEOPAUIXzDy9itkkHYrF4zrUt+pjTDX5FHZfITnvXUAxm6yhRouz8rY2X7ffeBWgDnpimDnHZF6KJp38vNrOp6XBYjQs3wTA1KK2p9K8GMi1v+QP2dnH+fG9IhEbpsAlzq3lAvFdVZBSzCbujqyj8b9Xg2ppo9250SMI0FZo="
aws_region = "us-east-1"

# Create a session using the configured credentials and region
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=aws_region
)

# Create an S3 client using the session
s3_client = session.client('s3')

# Specify the S3 bucket name and object key
bucket_name = 'project-1-2-3'
object_key = 'output_table.csv'

# Put the object into the S3 bucket
try:
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=csv_buffer.getvalue()
    )
    print("DataFrame uploaded as CSV to S3.")
except Exception as e:
    print("Failed to upload DataFrame as CSV to S3:", e)


# COMMAND ----------

import boto3
import pandas as pd

# Configure the AWS credentials
aws_access_key_id="ASIAUR75QRCKJZFNLZE3"
aws_secret_access_key="tXw91RrWlxY+JU39C4hfSaJH7arC5SnrrppDoN30"
aws_session_token="FwoGZXIvYXdzEDcaDEL6AvoIHgMct5lzxCLIAdAP5QImT0GzgXmNnNPStw+CAJu67b/0XUs/xxhBCyq2360fjyhbthdNtkaObh3IqB1o8TBbMFR9IFIQ4BP+u4YmCZ1aK/JhGZZhJILd0E24m5gn2AcoRXYpIHJwCkHRNWCkjZU2J8HEZjtwj01+LlAV4wnK1p1tx/SXlgx263hJEOPAUIXzDy9itkkHYrF4zrUt+pjTDX5FHZfITnvXUAxm6yhRouz8rY2X7ffeBWgDnpimDnHZF6KJp38vNrOp6XBYjQs3wTA1KK2p9K8GMi1v+QP2dnH+fG9IhEbpsAlzq3lAvFdVZBSzCbujqyj8b9Xg2ppo9250SMI0FZo="


# Configure the AWS region
aws_region = 'us-east-1'

# Create a session using the configured credentials and region
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=aws_region
)

# Create an S3 client using the session
s3_client = session.client('s3')

# Specify the bucket name and object key (file path) of the file you want to read
bucket_name = 'project-1-2-3'
file_key = 'output_table.csv'

# Read the file from S3
try:
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_key
    )
    s3_got_df = pd.read_csv(response['Body'])
    
    display(s3_got_df)
except Exception as e:
    print("Failed to read file from S3:", e)




# COMMAND ----------

print(s3_got_df.count())
