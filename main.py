import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', "-bn", type=str, help='Name of S3 bucket')
parser.add_argument('--url', type=str, help='link to download file')
parser.add_argument('--file_name', "-fn", type=str, help='uploaded file name')
parser.add_argument('--tool', '-t', type=str, help='choose function')
parser.add_argument('--filepath', "-fp", type=str, help='file path for upload')
parser.add_argument('--multipart_threshold', "-mth", type=int, default=5 * 1024 * 1024 * 1024, help='Multipart threshold in bytes (default: 5GB)')
parser.add_argument('--days', '-d', type=int, help='number of days when object will be deleted')
parser.add_argument('--memetype', '-mt', type=str, help='memetype which is allowed to upload')
args = parser.parse_args()
s3 = boto3.client('s3')
from hashlib import md5
from time import localtime 
load_dotenv()
s3 = boto3.client('s3')
def init_client():
    try:
        client = boto3.client("s3",
                              aws_access_key_id=getenv("aws_access_key_id"),
                              aws_secret_access_key=getenv(
                                  "aws_secret_access_key"),
                              aws_session_token=getenv("aws_session_token"),
                              region_name=getenv("aws_region_name")
                              #  config=botocore.client.Config(
                              #      connect_timeout=conf.remote_cfg["remote_timeout"],
                              #      read_timeout=conf.remote_cfg["remote_timeout"],
                              #      region_name=conf.remote_cfg["aws_default_region"],
                              #      retries={
                              #          "max_attempts": conf.remote_cfg["remote_retries"]}
                              )
        # check if credentials are correct
        return client
    except ClientError as e:
        logging.error(e)
    except:
        logging.error("Unexpected error")
 
 
def list_buckets(s3_client):
    try:
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html
        return s3_client.list_buckets()
    except ClientError as e:
        logging.error(e)
        return False
 
 
def create_bucket(s3_client, bucket_name, region=getenv("aws_region_name")):
    # Create bucket
    try:
        location = {'LocationConstraint': region}
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
        response = s3_client.create_bucket(
            Bucket=args.bucket_name,
            CreateBucketConfiguration=location
        )
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False
 
 
def delete_bucket(s3_client, bucket_name):
    # Delete bucket
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_bucket.html
        response = s3_client.delete_bucket(Bucket=args.bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False
 
 
def bucket_exists(s3_client, bucket_name):
    try:
        response = s3_client.head_bucket(Bucket=args.bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False
 
 
def download_file_and_upload_to_s3(s3_client, bucket_name, url, file_name, keep_local=False):
    from urllib.request import urlopen
    import io
    import urllib
    url = args.url
    format=urllib.request.urlopen(url).info()['content-type']
    format=format.split('/')
    formatlist = ["bmp", "jpg", "jpeg", "png", "webp", "mp4"]
    if format[1] in formatlist:
        with urlopen(args.url) as response:
            content = response.read()
            try:
                s3_client.upload_fileobj(
                    Fileobj=io.BytesIO(content),
                    Bucket=args.bucket_name,
                    ExtraArgs={'ContentType': 'image/jpg'},
                    Key=args.file_name
                )
                print("ფოტო აიტვირთა")
            except Exception as e:
                logging.error(e)
    
        if keep_local:
            with open(file_name, mode='wb') as jpg_file:
                jpg_file.write(content)
    else:
        print("ამ გაფართოების მქონე ფაილის ატვირთვა აკრძალულია")
    # public URL
    return "https://s3-{0}.amazonaws.com/{1}/{2}".format(
        'us-west-2',
        bucket_name,
        file_name
    )
 
def set_object_access_policy(s3_client, bucket_name, file_name):
    try:
        response = s3_client.put_object_acl(
            ACL="public-read",
            Bucket=args.bucket_name,
            Key=args.file_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False
 
def generate_public_read_policy(bucket_name):
    import json
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{args.bucket_name}/*",
            }
        ],
    }
 
    return json.dumps(policy)
 
def create_bucket_policy(s3_client, bucket_name):
    s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=generate_public_read_policy(args.bucket_name)
    )
    print("Bucket policy შეიქმნა წარმატებით")
 
def read_bucket_policy(s3_client, bucket_name):
    try:
        policy = s3_client.get_bucket_policy(Bucket=args.bucket_name)
        policy_str = policy["Policy"]
        print(policy_str)
    except ClientError as e:
        logging.error(e)
        return False
def upload_file(s3_client, bucket_name, file_name, filepath):
    with open(args.filepath, 'rb') as f:
        s3_client.upload_fileobj(f, args.bucket_name, args.file_name)
    print(f'{args.file_name} ფაილი აიტვირთა წარმატებით')
def lifecycle(s3_client, bucket_name , days):
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'Delete after {} days'.format(args.days),
                'Status': 'Enabled',
                'Prefix': '',
                'Expiration': {
                    'Days': args.days
                }
            }
        ]
    }
    s3_client.put_bucket_lifecycle_configuration(Bucket=args.bucket_name, LifecycleConfiguration=lifecycle_config)
    print(f'ბაკეტი {args.bucket_name} წაიშლება {args.days} დღეში ')
def big_file_upload(s3_client, bucketname, file_name, filepath, multipart_threshold):
    config = TransferConfig(multipart_threshold=args.multipart_threshold)
    with open(args.filepath, 'rb') as f:
        object_key = args.file_name or args.filepath.split('/')[-1]
        s3_client.upload_fileobj(f, args.bucket_name, object_key, Config=config)
    print(f'{args.file_name} აიტვირთა წარმატებით')
if __name__ == "__main__":
    s3_client = init_client()
if args.tool == 'init_client' or args.tool == 'ic':
    init_client()
if args.tool == 'list_bucket' or args.tool == 'lb':
    buckets = list_buckets(s3_client)
    if buckets:
        for bucket in buckets['Buckets']:
            print(f'  {bucket["Name"]}')
if args.tool == 'create_bucket' or args.tool == 'cb':
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'] == args.bucket_name:
            print(f'The bucket {args.bucket_name} უკვე არსებობს.')
            break
    else:
        s3_client.create_bucket(Bucket=args.bucket_name)
        print(f'bucket {args.bucket_name} შეიქმნა.')
if args.tool == 'delete_bucket' or args.tool == 'db':
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'] == args.bucket_name:
            response = s3_client.delete_bucket(Bucket=args.bucket_name)
            print(f'bucket {args.bucket_name} წაიშალა')
            break
    else:
      print(f'bucket {args.bucket_name} არ არსებობს.')
if args.tool == 'bucket_exists' or args.tool == 'be':
    try:
        bucket_exists(s3_client, args.bucket_name)
        print(f'The bucket {args.bucket_name} არსებობს.')
    except:
        print(f'The bucket {args.bucket_name} არ არსებობს')  
if args.tool == 'set_object_access_policy' or args.tool == 'soap':
    try:
        set_object_access_policy(s3_client, args.bucket_name, args.file_name)
        print("object access policy დაემატა წარმატებით")
    except:
        print("object access policy ვერ დაემატა")
if args.tool == 'generate_public_read_policy' or args.tool == 'gprp':
    try:
        generate_public_read_policy(args.bucket_name)
        print("public read policy დაგენერირდა")
    except:
        print("public read policy ვერ დაგენერირდა")
if args.tool == 'read_bucket_policy' or args.tool == 'rbp':
    read_bucket_policy(s3_client, args.bucket_name)
if args.tool == 'create_bucket_policy' or args.tool == 'cbp':
    create_bucket_policy(s3_client, args.bucket_name)
if args.tool == "download_file_and_upload_to_s3" or args.tool == 'du':
    download_file_and_upload_to_s3(s3_client, args.bucket_name, args.url, args.file_name, keep_local=False)
if args.tool == "upload" or args.tool == 'u':
    meme=args.filepath.split('.')[-1]
    if args.memetype == meme:
        upload_file(s3_client, args.bucket_name, args.file_name, args.filepath)
    else:
        print(f' {meme} გაფართების მქონე ფაილის ატვირთვა არ არის ნებადართული')
if args.tool == 'lifecycle' or args.tool == 'lc':
    lifecycle(s3_client , args.bucket_name, args.days)
if args.tool == 'big_file_upload' or args.tool == 'bfu':
    meme=args.filepath.split('.')[-1]
    if args.memetype == meme:
        big_file_upload(s3_client, args.bucket_name, args.file_name, args.filepath, args.multipart_threshold)
    else:
        print(f'{meme} გაფართების მქონე ფაილის ატვირთვა არ არის ნებადართული')