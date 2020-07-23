# SFTP-S3-Glue-Ingestion-Python
Glue Batch ingestion job to move files from file server to S3
This code is part of my article on Step-By-Step guide on [how to design a data lake using AWS Glue, Athena, Lambda and QuickSight](https://towardsdatascience.com/a-complete-guide-on-serverless-data-lake-using-aws-glue-athena-and-quicksight-3a8a24cfa4af?source=friends_link&sk=3fc81d6d22d0361c1fb4e9f83c739619)

This AWS Glue Python Shell Job is used to upload intial Batch of files on AWS S3 Bucket. This Glue Job also handles CDC, which will upload updated files on the S3 bucket when Glue Job is run after initial load. This code uses AWS SDK Boto3. Following are the key features:
* Fetch credentials from SSM Parameter Store (file server credentials, folder path, S3 bucket)
* Connect to SFTP file server and upload files (which are not already present on S3 and are not updated since the last time Glue job ran)
* Automatic Handling of Failed Scenarios (Retries) and Exception Handling
* Handles multipart upload to s3 automatically, if file size is greater than 100MB
