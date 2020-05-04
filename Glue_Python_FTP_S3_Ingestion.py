import boto3
import paramiko
import datetime
from boto3.s3.transfer import TransferConfig
from boto3.exceptions import S3UploadFailedError
from paramiko import BadHostKeyException, ssh_exception

# Fetch credentials from SSM
print("fetching credentials from SSM!")
ssm = boto3.client('ssm', region_name="us-east-2")
try:
    ftp_host = ssm.get_parameter(Name='ftp_host')['Parameter']['Value']
    ftp_port = ssm.get_parameter(Name='ftp_port')['Parameter']['Value']
    ftp_username = ssm.get_parameter(Name='ftp_username')['Parameter']['Value']
    ftp_password = ssm.get_parameter(Name='ftp_password')['Parameter']['Value']
    ftp_file_path = ssm.get_parameter(Name='ftp_file_path')['Parameter']['Value']
    s3_bucket = ssm.get_parameter(Name='s3_bucket')['Parameter']['Value']
    last_etl_execution_time = ssm.get_parameter(Name='Last_ETL_Execution_Time')['Parameter']['Value']
except ssm.exceptions as error:
    print("Error fetching parameter from parameter store, ", error)
    exit(1)
print("parameters fetched successfully")

# Establishing connection with file server
print("Connecting to file server!")
try:
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(
        paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=ftp_host, username=ftp_username, password=ftp_password, port=ftp_port)
except Exception as error:
    print("Error occurred while connecting to ftp server: ", error)
    exit(1)
print("Established connection to file server successfully")

s3 = boto3.client('s3', region_name="us-east-2")
MB = 1024 ** 2
config = TransferConfig(multipart_threshold=100 * MB, multipart_chunksize=10 * MB)
sftp = ssh_client.open_sftp()
last_etl_execution_time = datetime.datetime.strptime(last_etl_execution_time, '%Y-%m-%d %H:%M:%S')

'''
Iterating over file server directory and comparing last modified date of file
We will be uploading those files whose last_modified_date is >= last_etl_execution_time of python shell job
'''
print("Iterating over files on file server!")
for file in sftp.listdir(ftp_file_path):
    file_path = ftp_file_path + "/" + file
    utime = sftp.stat(file_path).st_mtime
    last_modified = datetime.datetime.fromtimestamp(utime)
    if last_modified >= last_etl_execution_time:
        file_to_upload = sftp.file(file_path, mode='r')
        try:
            print("Uploading File %s" % file)
            s3.upload_fileobj(file_to_upload, s3_bucket, file, Config=config)
        except S3UploadFailedError as e:
            print("Failed to upload %s to %s: %s" % (file, '/'.join([s3_bucket]), e))
print("Uploaded files on S3 successfully")

# updating last_ETL_Running_Job_Time in SSM Parameter store
print("Updating last_ETL_Running_Job_Time in SSM Parameter store")
try:
    ssm.put_parameter(Name='Last_ETL_Execution_Time', Value=str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                      Type='String', Overwrite=True)
except ssm.exceptions:
    print("Error fetching parameter from parameter store, ", error)
    exit(1)
print("Job completed successfully!!!")
