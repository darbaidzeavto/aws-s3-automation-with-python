import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import argparse
import json
import magic
import os
import logging
import errno
import sys
import os
import threading
import ntpath
from pathlib import Path
from hurry.filesize import size, si
from urllib.request import urlopen, Request
from random import choice
parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', "-bn", type=str, help='Name of S3 bucket')
parser.add_argument('--url', type=str, help='link to download file')
parser.add_argument('--file_name', "-fn", type=str, help='uploaded file name')
parser.add_argument('--tool', '-t', type=str, help='choose function')
parser.add_argument('--filepath', "-fp", type=str, help='file path for upload')
parser.add_argument('--multipart_threshold', "-mth", type=int, default=5 * 1024 * 1024 * 1024, help='Multipart threshold in bytes (default: 5GB)')
parser.add_argument('--days', '-d', type=int, help='number of days when object will be deleted')
parser.add_argument('--memetype', '-mt', type=str, help='memetype which is allowed to upload')
parser.add_argument('-del', dest='delete', action='store_true', help='Delete the file')
parser.add_argument('-vers', dest='versioning', action='store_true', help='check versioning')
parser.add_argument('-verslist', dest='versionlist', action='store_true', help='version list')
parser.add_argument('-prevers', dest='previous_version', action='store_true', help='roll back to previous version')
parser.add_argument('-orgobj', dest='organize_objects', action='store_true', help='put files in folder according extention type')
parser.add_argument('--inspire', nargs='?', const='true', help='Quote from api' )
parser.add_argument('-save', action='store_true', help='upload quote.json to s3 bucket' )
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
def list_objects(s3_client, bucket_name):
    response = s3_client.list_objects(Bucket=args.bucket_name)
    for obj in response['Contents']:
        print(obj['Key'])
def previous_version(s3_client, bucket_name, file_name):
    try:
        response = s3_client.get_object(Bucket=args.bucket_name, Key=args.file_name)
        latest_version_id = response['VersionId']
        response = s3_client.list_object_versions(Bucket=args.bucket_name, Prefix=args.file_name)
        previous_version_id = response['Versions'][1]['VersionId']
        response = s3_client.copy_object(
            Bucket=args.bucket_name,
            Key=args.file_name,
            CopySource={'Bucket': args.bucket_name, 'Key': args.file_name, 'VersionId': previous_version_id})
        print(f'{args.file_name} ფაილი დაუბრუნდა წინა ვერსიას')
    except:
        print(f'{args.file_name} ფაილის წინა ვერსიაზე ვერ დაბრუნდა')
def version_list(s3_client, bucket_name, file_name):
    response = s3_client.list_object_versions(Bucket=args.bucket_name, Prefix=args.file_name)
    num_versions = len(response['Versions'])
    dates = [v['LastModified'] for v in response['Versions']]
    print(f'Bucket name: {args.bucket_name}')
    print(f'File name: {args.file_name}')
    print(f'Number of versions: {num_versions}')
    print('Creation dates of versions:')
    for date in dates:
        print(date)
def versioning(s3_client, bucket_name):
    output = s3_client.get_bucket_versioning(Bucket=args.bucket_name,)
    try:
        status = output['Status']
        print(f'ბაკეტზე {args.bucket_name} ვერსიონირება ჩართლია')
    except:
        print(f'ბაკეტზე {args.bucket_name} ვერსიონირება არ არის ჩართული')
def delete_file(s3_client, bucket_name, filename):
    response = s3_client.delete_object(Bucket=args.bucket_name, Key=args.file_name)
    print(f'{args.file_name} ფაილი წაიშალა')
def organize_objects(s3_client, bucket_name):
    counter = {}
    response = s3_client.list_objects_v2(Bucket=args.bucket_name)
    if response is not None:
        contents = response.get('Contents', [])
        for obj in contents:
            key = obj['Key']
            if '.' in key:
                extension_name = key.split(".")[-1]
                if not counter.get(extension_name):
                    counter[extension_name] = 1
                    destination_key = extension_name + '/' + key
                    s3_client.put_object(Bucket=args.bucket_name, Key=extension_name+'/')
                    s3_client.copy_object(Bucket=args.bucket_name, CopySource={'Bucket': args.bucket_name, 'Key': key}, Key=destination_key)
                    s3_client.delete_object(Bucket=args.bucket_name, Key=key)
                else:
                    counter[extension_name] += 1
                    destination_key = extension_name + '/' + key
                    s3_client.copy_object(Bucket=args.bucket_name, CopySource={'Bucket': args.bucket_name, 'Key': key}, Key=destination_key)
                    s3_client.delete_object(Bucket=args.bucket_name, Key=key)

    else:
        print('Response is None')
    print(counter)
def upload_with_magic(s3_client, bucket_name, file_name, filepath):
    ext = magic.from_file(args.filepath, mime=True).split('/')[-1] + '/' + args.file_name
    with open(args.filepath, 'rb') as f:
        s3_client.upload_fileobj(f, args.bucket_name, ext)
        print(f'{args.file_name} წარმატებით აიტვირთა')
def delete_old_versions(s3_client, bucket_name, file_name, days):
    from datetime import date
    from datetime import datetime
    response = s3_client.list_object_versions(Bucket=args.bucket_name, Prefix=args.file_name)
    dates = [v['LastModified'] for v in response['Versions']]
    today = str(date.today())
    for version in response['Versions']:
        id = version['VersionId']
        responses = s3_client.head_object(Bucket=args.bucket_name, Key=args.file_name, VersionId=id)
        last_modified = responses['LastModified']
        date = str(last_modified)
        date = date.split(' ', 1)[0]
        d2 = datetime.strptime(date, "%Y-%m-%d")
        d1 = datetime.strptime(today, "%Y-%m-%d")
        delta = d1 - d2
        if delta.days > args.days:
            s3_client.delete_object(Bucket=args.bucket_name, Key=args.file_name, VersionId=id)
            print(f'ვერსია {id} წაიშალა რადგან {args.days} დღეზე მეტი ხნის წინ შეიქმნა')
def upload_file_multipart(s3_client, filepath, bucket_name, file_name, metadata=None):
    MP_THRESHOLD = 1
    MP_CONCURRENCY = 5
    MAX_RETRY_COUNT = 3
    log = logging.getLogger('s3_uploader')
    log.setLevel(logging.INFO)
    format = logging.Formatter("%(asctime)s: - %(levelname)s: %(message)s", "%H:%M:%S")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(format)
    log.addHandler(stream_handler)
    log.info("Uploading [" + args.filepath + "] to [" + args.bucket_name + "] bucket ...")
    log.info("S3 path: [ s3://" + args.bucket_name + "/" + args.file_name + " ]")
    # Multipart transfers occur when the file size exceeds the value of the multipart_threshold attribute
    if not Path(args.filepath).is_file:
        log.error("File [" + file + "] does not exist!")
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file)
 
    if args.file_name is None:
        log.error("object_path is null!")
        raise ValueError("S3 object must be set!")
    
    
    GB = 1024 ** 3
    mp_threshold = MP_THRESHOLD*GB
    concurrency = MP_CONCURRENCY
    transfer_config = TransferConfig(multipart_threshold=mp_threshold, use_threads=True, max_concurrency=concurrency)
 
    login_attempt = False
    retry = MAX_RETRY_COUNT
     
    while retry > 0:
        try:
            s3_client.upload_file(args.filepath, args.bucket_name, args.file_name, Config=transfer_config, ExtraArgs=metadata)
            sys.stdout.write('\n')
            log.info("File [" + args.file_name + "] uploaded successfully")
            log.info("Object name: [" + args.file_name + "]")
            retry = 0
         
        except ClientError as e:
            log.error("Failed to upload object!")
            log.exception(e)
            if e.response['Error']['Code'] == 'ExpiredToken':
                log.warning('Login token expired')
                retry -= 1
                log.debug("retry = " + str(retry))
                login_attempt = True
                login()
            else:
                log.error("Unhandled error code:")
                log.debug(e.response['Error']['Code'])
                raise
 
        except boto3.exceptions.S3UploadFailedError as e:
            log.error("Failed to upload object!")
            log.exception(e)
            if 'ExpiredToken' in str(e):
                log.warning('Login token expired')
                log.info("Handling...")
                retry -= 1
                log.debug("retry = " + str(retry))
                login_attempt = True
                login()
            else:
                log.error("Unknown error!")
                raise
 
    if login_attempt:
        raise Exception("Tried to login " + str(MAX_RETRY_COUNT) + " times, but failed to upload!")
def static_website(s3_cient, bucket_name):
    with open(args.filepath, 'rb') as f:
        s3_cient.upload_fileobj(f, args.bucket_name, args.file_name, ExtraArgs={'ContentType': 'text/html'})
    website_configuration = {
    'ErrorDocument': {'Key': 'error.html'},
    'IndexDocument': {'Suffix': 'index.html'},
    }
    s3_client.put_bucket_website(Bucket=args.bucket_name ,WebsiteConfiguration=website_configuration)
    print("სტატიკური ვებსაიტი დაიჰოსტა წარმატებით")
def upload_and_host(s3_client, bucket_name, filepath):
    create_bucket_policy(s3_client, args.bucket_name)
    local_folder_path = args.filepath
    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            # Use relative path for S3 key, e.g. folder/subfolder/file.txt
            s3_key = os.path.relpath(local_file_path, local_folder_path).replace('\\', '/')
            s3_client.upload_file(local_file_path, args.bucket_name, s3_key, ExtraArgs={'ContentType': 'text/html'})
                
    website_configuration = {
        'ErrorDocument': {'Key': 'error.html'},
        'IndexDocument': {'Suffix': 'index.html'},
        }
    s3_client.put_bucket_website(Bucket=args.bucket_name, WebsiteConfiguration=website_configuration)
    response = s3_client.get_bucket_location(Bucket=args.bucket_name)

    region = response['LocationConstraint']

    if not region:
        region = 'us-east-1'
def analytics(data):
  stats = {"quotes": 0}
  for index, each in enumerate(data):

    if not stats.get(each["author"]):
      stats[each["author"]] = {"quote_index": [index], "quotes_avaliable": 1}
    else:
      stats[each["author"]]['quote_index'].append(index)
      stats[each["author"]]['quotes_avaliable'] += 1
    stats['quotes'] += 1

  return stats
def main():
  headers = {
  'user-agent':
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
   }
  json_result = []
  with urlopen(
      Request("https://type.fit/api/quotes", data=None,
              headers=headers)) as response:
    json_result = json.loads(response.read().decode())
    if args.inspire == "true":
        print(json.dumps(choice(json_result)["text"], indent=4))
    else:
        json_string = json.dumps(json_result)
        list_of_quotes = json.loads(json_string)
        for quote in list_of_quotes:
            if quote["author"] == args.inspire:
                print(quote["text"])
                if args.save == True:
                    with open("quotes.json", "w") as f:
                        json.dump(quote, f)
                    with open("quotes.json", "rb") as f:
                        s3_client.upload_fileobj(f, args.bucket_name, "quotes.json")
                        print(f'quotes.json აიტვირთა {args.bucket_name} ბაკეტში')
                break

    print(f'http://{args.bucket_name}.s3-website-{region}.amazonaws.com')
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
if args.tool == list_objects or args.tool =='lo':
    list_objects(s3_client, args.bucket_name)
if args.delete == True:
    delete_file(s3_client, args.bucket_name, args.file_name)
if args.versioning == True:
    versioning(s3_client, args.bucket_name)
if args.versionlist == True:
    version_list(s3_client, args.bucket_name, args.file_name)
if args.previous_version == True:
    previous_version(s3_client, args.bucket_name, args.file_name)
if args.organize_objects == True:
    organize_objects(s3_client, args.bucket_name)
if args.tool == "upload_with_magic" or args.tool == "uwm":
    upload_with_magic(s3_client, args.bucket_name, args.file_name, args.filepath)
if args.tool == "delete_old_versions" or args.tool == "dov":
    delete_old_versions(s3_client, args.bucket_name, args.file_name, args.days)
if args.tool == "upload_file_multipart" or args.tool == "ufm":
    name = args.filepath.split('/')[-1]
    type = args.filepath.split('.')[-1]
    size = os.path.getsize(args.filepath)/(1024 ** 3)
    metadata = {}
    metadata['Metadata'] = {'name': name, 'type': type,'size': f'{size}GB'}
    upload_file_multipart(s3_client, args.filepath, args.bucket_name, args.file_name, metadata=metadata)
if args.tool == "static_website" or args.tool == "sw":
    static_website(s3_client, args.bucket_name)
if args.tool == upload_and_host or args.tool == "uh":
    upload_and_host(s3_client, args.bucket_name, args.filepath)
if args.inspire == "true" or args.inspire is not None:
    main()


