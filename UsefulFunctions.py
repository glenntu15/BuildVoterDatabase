import sys
import os
import boto3
from Config_Defaults import DefaultConfigurations

STRICT_ERROR_PRINT = True

def error_print(msg:str, strict:bool = STRICT_ERROR_PRINT):
    print(f"(!) ERROR: {msg}")
    if strict:
        sys.exit(1)

def is_pow_of_2(n):
    return type(n) == int and n > 0 and n&(n-1) == 0    # Clever Bitwise approach: O(1)

"""
AWS S3 Functions:
"""
boto3_client = None

def init_boto3():
    global boto3_client

    boto3_client = boto3.client("s3")

def list_buckets():
    try:
        response = boto3_client.list_buckets()
        if response:
            print("Buckets:")
            for bucket in response["Buckets"]:
                print(f"\t{bucket['Name']}")
            print()
    except Exception as e:
        error_print(e)

def create_bucket(bucket_name:str = "", region:str = ""):
    try:
        if len(bucket_name)==0:
            bucket_name = DefaultConfigurations.S3_BUCKET_NAME
        boto3_client.create_bucket(Bucket = bucket_name,
            CreateBucketConfiguration = {"LocationConstraint": region if region else DefaultConfigurations.S3_REGION})
    except Exception as e:
        error_print(e)

def delete_bucket(bucket_name:str):
    try:
        boto3_client.delete_bucket(Bucket = bucket_name)
    except Exception as e:
        error_print(e)

def upload_string(file_name:str, bucket_name:str, data_string:str):
    if len(data_string) == 0:
        error_print("`data_string` given to insert was empty.")

    try:
        boto3_client.put_object(
            Body = data_string,
            Bucket = bucket_name,
            Key = file_name
        )
    except Exception as e:
        error_print(e)

# Replaced by upload_string():
'''
def upload_file(file_name:str, bucket_name:str, object_name:str = ""):
    if not object_name:
        object_name = os.path.basename(file_name)

    try:
        response = boto3_client.upload_file(file_name, bucket_name, object_name)
        print(response)
    except Exception as e:
        error_print(e)
'''

def download_file(file_name:str, bucket_name:str, object_name:str = ""):
    if not object_name:
        object_name = os.path.basename(file_name)
    
    original_dir = os.getcwd()
    download_dir = 'data_s3_searched'
    if os.path.basename(original_dir) != download_dir:
        os.chdir( os.path.join(original_dir, download_dir) )

    try:
        boto3_client.download_file(bucket_name, object_name, file_name)
    except Exception as e:
        error_print(e)

    os.chdir(original_dir)

def delete_file(bucket_name:str, file_name:str):
    try:
        boto3_client.delete_object(Bucket = bucket_name,
                                   Key = file_name)
    except Exception as e:
        error_print(e)


def test_s3_functions():
    ## MUST `init_boto3()` Prior to All AWS S3 Functions
    # init_boto3()

    ## Test Variables
    test_bucket_name = "boto3-test-globallyuniqueid"
    test_file_name = "test-file-name"
    test_string = "testing123, somevalue456, \"somestring789\", 0"

    ## Get List of Buckets
    '''
    list_buckets()
    '''

    ## Create Bucket
    '''
    create_bucket(test_bucket_name)
    print("Success: bucket was created!")
    list_buckets()
    '''

    ## Delete Bucket
    '''
    delete_bucket(test_bucket_name)
    print("Success: bucket was deleted!")
    list_buckets()
    '''


    ## Upload String to Bucket
    '''
    upload_string(test_file_name, test_bucket_name, test_string)
    print("Success: inserted string!")
    '''

    ## Upload File to Bucket (Not Used)
    '''
    upload_file(test_file_name, test_bucket_name)
    print("bucket file uploaded successfully..!")
    '''

    ## Download File from Bucket
    '''
    download_file(test_file_name, test_bucket_name)
    print("Success: bucket file was downloaded!")
    '''

    ## Delete File from Bucket
    '''
    delete_file(test_bucket_name, test_file_name)
    print("Success: Bucket file was deleted!")
    '''
    

# if __name__ == "__main__":
#     test_s3_functions()