
""" aws s3 presign s3://avlil/lil1.txt --expires-in 60
    default expiry 3600 secs. 1 hour.. Max expiry: 7 days
    Can expire even before the expiry is set:
    https://aws.amazon.com/premiumsupport/knowledge-center/presigned-url-s3-bucket-expiration/
    What if I need to expire it earlier??
    Can upload using signed urls as well..
"""
import os
import boto3
from botocore.exceptions import ClientError

ACCESS_KEY = 'AWS_ACCESS_KEY_ID'
SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
PRI_BUCKET_NAME = 'avlil'
TRANSIENT_BUCKET_NAME = 'avlil2'
F1 = "lil1.txt"
F2 = "lil2.txt"
F3 = "lil3.txt"
DIR = "F:/aws/s3/local"
DOWN_DIR = "F:/aws/s3/download"

def upload_file(bucket, directory, file, s3, s3path=None):
    file_path = directory + '/' + file
    remote_path = s3path
    if remote_path is None:
        remote_path = file
    try:
        s3.Bucket(bucket).upload_file(file_path, remote_path)
    except ClientError as ce:
        print('error', ce)

def prevent_public_access(bucket, s3):
    try:
        s3.meta.client.put_public_access_block(Bucket=bucket,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            })
    except ClientError as ce:
        print('error', ce)


def create_bucket(name, s3, secure=False):
    try:
        s3.create_bucket(Bucket=name,
                         CreateBucketConfiguration={
                             'LocationConstraint': 'ap-south-1'}
                         )
        if secure:
            prevent_public_access(name, s3)
    except ClientError as ce:
        print('error', ce)

def upload_presigned(bucket, key, expiration_seconds, s3):
    try:
        response = s3.meta.client.generate_presigned_url('put_object', Params={
            'Bucket': bucket,
            'Key': key
        }, ExpiresIn=expiration_seconds)
        print(response)

        print(f"curl -i --request PUT --upload-file plain.txt '{response}'")
    except ClientError as ce:
        print('error', ce)

def main():
    """entry point"""
    access = os.getenv(ACCESS_KEY)
    secret = os.getenv(SECRET_KEY)

    s3 = boto3.resource('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    print(f"Creating private bucket: {PRI_BUCKET_NAME}")
    create_bucket(PRI_BUCKET_NAME, s3, True)

    print(f'Uploading 3 files from {DIR}...')
    upload_file(PRI_BUCKET_NAME, DIR, F1, s3)
    upload_file(PRI_BUCKET_NAME, DIR, F2, s3)
    upload_file(PRI_BUCKET_NAME, DIR, F3, s3)

    print('Generating a public link to 3rd file (1 min validity) to upload...')
    upload_presigned(PRI_BUCKET_NAME, F3, 60, s3)

if __name__ == '__main__':
    main()
