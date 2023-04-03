README
This code provides a command-line interface for performing various tasks related to Amazon S3. The code requires Python 3.x, the boto3, dotenv, argparse, logging, botocore and magic libraries to be installed.

Prerequisites
Python 3.x
boto3 library
dotenv library
argparse library
logging library
botocore library
magic library


Install the required libraries using the command pip install boto3 dotenv.
Set your AWS credentials as environment variables by creating a .env file in the root directory of the project and adding the following lines:
makefile
Copy code
aws_access_key_id=<your_access_key>
aws_secret_access_key=<your_secret_key>
aws_session_token=<your_session_token>
aws_region_name=<your_region_name>
Replace <your_access_key>, <your_secret_key>, <your_session_token>, and <your_region_name> with your actual AWS credentials.

Usage
List Buckets
To list all the buckets in your S3 account, run the following command:

python main.py -t list_buckets(lb)

Create Bucket
To create a new bucket in S3, run the following command:
python main.py --tool(-t) create_bucket(cb) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the desired name of your bucket.

Delete Bucket
To delete a bucket from S3, run the following command:
python main.py -t delete_bucket(db) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the name of the bucket you want to delete.

bucket exists 
to check s3 bucket exists or not :
python main.py -t bucket_exists(be) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the name of the bucket

Set Object Access Policy
To set the access policy for a file in S3, run the following command:
python main.py.py --tool(-t) set_object_access_policy(soap) --bucket_name(-bn) <bucket_name> --file_name(-fn) <file_name>
Replace <bucket_name> with the name of the bucket that contains the file, and <file_name> with the name of the file you want to set the access policy for.

generete public read policy
to generate public read  policy in s3, run the following command:
python main.py -tool(-t) generate_public_read_policy(gprp) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the name of the bucket

read bucket policy 
to read bucket policy in s3, run the following command:
python --tool(-t) read_bucket_policy(rbp) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the name of the bucket

create bucket policy
to create bucket policy, run the following command:
python main.py --tool(-t) create_bucket_policy(cbp) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the name of the bucket

download and Upload File
To download and upload a file to a bucket in S3, run the following command:
python main.py --tool(-t) download_file_and_upload_to_s3(du) --bucket_name(-bn) <bucket_name> --file_name(-fn) <file_name> --url <url>
Replace <bucket_name> with the name of the bucket you want to upload the file to, <file_name> with the desired name of the file in S3, <url> with the URL of the file you want to upload

upload file 
to upload file in s3, run the following command:
python main.py --tool(-t) upload(u) --bucket_name(-bn) <bucket_name> --file_name(-fn) <file_name> --filepath(-fp) <filepath> --memetype(-mt) <memetype>
Replace <bucket_name> with the name of the bucket you want to upload the file to, <file_name> with the desired name of the file in S3, <filepath> with file path where the file is located, <memetype> which type of file is allowed to upload

delete object
to delete object in s3 bucket,run the following command:
python main.py -del --bucket_name(-bn) <bucket_name> --file_name(-fn) <file_name>
Replace <bucket_name> with the name of the bucket,<file_name> withe the name of the file you want to delete

versioning
to enable versioning the bucket,run the folllowing command:
python main.py --bucket_name(-bn) <bucket_name> -vers
Replace <bucket_name> with the name of the bucket

version list 
to list versions of the file in bucket, run the following command:
python main.py --bucket_name(-bn) <bucket_name> --file_name(-fn) <file_name> -verslist
Replace <bucket_name> with the name of the bucket,<file_name> withe the name of the file

previous version
to roll back file to previous version, run the following command:
python main.py --bucke_name(-bn) <bucket_name> --file_name(-fn) <file_name> -prevers
Replace <bucket_name> with the name of the bucket,<file_name> with the name of the file

lifecycle
create lifecyle policy for bucket,run the following command:
python main.py --tool(-t) lifecycle(lc) --bucket_name(-bn) <bucket_name> --days(-d) <days>
Replace <bucket_name> with the name of the bucket, <days> with the number of days after which you want it to be deleted bucket

list objects
to list objects in s3 bucket,run the following command:
python main.py --tool(-t) list_object(lo) --bucket_name(-bn) <bucket_name>
Replace <bucket_name> with the name of the bucket

organize objects
to put files in folder according extention type in s3 bucket, run the following command:
python main.py --bucket_name(-bn) <backet_name> --orgobj
Replace <bucket_name> with the name of the bucket

upload with magic 
upload file with magiclib to put files in folder according extention,run the following command
python main.py --tool(-t) upload_with_magic(uwm) --bucket_name(-bn) <bucket_name>  --file_name(-fn) <file_name> --filepath(-fp) <filepath>
Replace <bucket_name> with the name of the bucket you want to upload the file to, <file_name> with the desired name of the file in S3, <filepath> with file path where the file is located
