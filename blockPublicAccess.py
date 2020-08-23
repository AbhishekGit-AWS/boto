#!/usr/bin/env python3

# -*-coding:utf-8 -*-

""" A python script for working with amazon S3.
    Creating a private bucket and uploading a few files.
    Access Keys and Id are configured in pyCharm"""
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

def main():
    """entry point"""
    access = os.getenv(ACCESS_KEY)
    secret = os.getenv(SECRET_KEY)
    s3 = boto3.resource('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    print(f"Creating primary bucket: {PRI_BUCKET_NAME}")
    create_bucket(PRI_BUCKET_NAME, s3, True)

    print(f'Uploading 3 files from {DIR}...')
    upload_file(PRI_BUCKET_NAME, DIR, F1, s3)
    upload_file(PRI_BUCKET_NAME, DIR, F2, s3)
    upload_file(PRI_BUCKET_NAME, DIR, F3, s3)

if __name__ == '__main__':
    main()
