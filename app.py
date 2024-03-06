import base64
import boto3
from datetime import datetime, timedelta
import json
import logging
import os
import requests
import snowflake.connector

"""
    The Lambda environment pre-configures a handler logging to stderr. 
    If a handler is already configured, basicConfig does not execute. 
    Thus we set the level directly.
"""
if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AwsInterface:
    """
    Core AWS class for fetching secrets related to AWS integration. 
    Invoked by lambda_handler function.
    args: region_name
    returns: None
    """
    def __init__(self, region_name):   
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name="secretsmanager", region_name=region_name)

    def get_secret(self, secret_name: str) -> str:
        response = self.client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return response["SecretString"]
        elif "SecretBinary" in response:
            return base64.b64decode(response["SecretBinary"]).decode("utf8")
        else:
            raise ValueError(
                "Invalid secret value key. Expected 'SecretString' or 'SecretBinary'.")

"""
Class to extract audio from a given url and upload to S3.
properties: url, s3_bucket, s3_key
method: download_audio, upload_to_s3, cleanup, extract_and_upload
"""
class AudioExtractor:
    def __init__(self, url, s3_bucket, s3_key):
        self.url = url
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.temp_file = "/tmp/temp_audio.mp3"
    """
    Method to download audio from a given url.
    args: None
    returns: None
    """
    def download_audio(self):
        try:
            response = requests.get(self.url) 
            response.raise_for_status()
            with open(self.temp_file, 'wb') as file:
                file.write(response.content)
        except Exception as e:
            logger.error("Error downloading audio: %s", e)
            raise e

    """
    Method to upload audio to S3.
    args: None
    returns: None
    """
    def upload_to_s3(self):
        try:
            s3 = boto3.client("s3")
            logger.debug(f"Putting {self.temp_file} to s3://{self.s3_bucket} with key {self.s3_key}")
            s3.upload_file((self.temp_file), self.s3_bucket, self.s3_key)
            logger.info(f"Audio uploaded to S3://{self.s3_bucket}/{self.s3_key}")
        except Exception as e:
            logger.error("Error uploading audio to S3: %s", e)
            raise e
    """
    Method to cleanup the temp file.
    args: None
    returns: None
    """
    def cleanup(self):
        if os.path.exists(self.temp_file):
            os.remove(self.temp_file)
    """
    Method to extract audio from a given url and upload to S3.
    args: None
    returns: None
    """
    def extract_and_upload(self):
        try:
            self.download_audio()
            self.upload_to_s3()
        except Exception as e:
            logger.error("Error extracting and uploading audio: %s", e)
            raise e
        finally:
            self.cleanup()
            logger.debug("Upload Completed. ")
#Main function
def lambda_handler(request, context):
    # Set the initial time
    start_time = datetime.utcnow()
    # Get config from request
    password = request["secrets"]["api_key"]
    user=request["secrets"]["user"]
    account=request["secrets"]["account"]
    uploaded_to_s3_datetime = request.get("state", {}).get("uploaded_to_s3_datetime")
    s3_bucket_name = "s3_bucket_name"
    aws_interface = AwsInterface(region_name='us-west-2')
    ssm_secret_dialpad=aws_interface.get_secret('api_keys')
    apikey = json.loads(ssm_secret_dialpad)['api_keys'] 
    # Create fivetran response and set initial values such as schema 
    response_fivetran = {
        "hasMore": False,
        "schema": {
            "audio_upload_status": {"primary_key": ["call_id", "uploaded_status_s3", "file_name"]}
            
        },
        "state": {"uploaded_to_s3_datetime": uploaded_to_s3_datetime},
        "insert": {"audio_upload_status": []}
    }
    data_for_update = []
    #connect to Snowflake
    ssm_secret=aws_interface.get_secret('/user_id/snowflake_password')
    password = json.loads(ssm_secret)['user_id/snowflake_password'] 
    ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account
    )
    cs = ctx.cursor()
    try:
        cs.execute("select  call_id,recording_url,file_name,file_folder,date_started  from <table_name> where uploaded_status_s3 = false order by call_id limit 2000")
        for (call_id,recording_url,file_name, file_folder, date_started ) in cs:
            logger.debug(f"call_id: {call_id}, recording_url: {recording_url},file_name:{file_name}")
            url=recording_url+"?apikey="+apikey
            s3_key= f"{file_folder}/{file_name}"
            audio_extractor = AudioExtractor(url, s3_bucket_name, s3_key)
            audio_extractor.extract_and_upload()
            uploaded_status_s3=True
            uploaded_to_s3_datetime = datetime.utcnow().isoformat()
            #add call_id, file_name, uploaded_status_s3, uploaded_to_s3_datetime to the response
            data_for_update.append({"call_id": call_id, "file_name": file_name, "uploaded_status_s3": uploaded_status_s3, "uploaded_to_s3_datetime": uploaded_to_s3_datetime})
            duration = datetime.utcnow() - start_time
            if duration > timedelta(minutes=14, seconds=30):
                logger.info("Run time exceeded 14 minutes and 30 seconds.exiting to prevent lambda timeout")
                break
        response_fivetran["insert"]["audio_upload_status"].extend(data_for_update)
        response_fivetran["state"]["uploaded_to_s3_datetime"] = datetime.utcnow().isoformat()
        response_fivetran["hasMore"] = False      
    except Exception as ex:
        logger.error("Exception caught : %s", ex)
        raise ex
    finally:
        cs.close()
        ctx.close()
        logger.debug("response_fivetran: %s", response_fivetran) 
    return response_fivetran

#local testing for lambda_handler
if __name__ == "__main__":
    # Instantiate an object of the AwsInterface class
    aws_interface = AwsInterface(region_name='us-west-2')
    ssm_secret=aws_interface.get_secret('api_keys')
    api_key = json.loads(ssm_secret)['api_keys']  
    request = {
    "secrets": {
        "api_key": api_key ,
        "user": "lambda_svc",
        "account": "pk83162"    
    },
     "state": {
        "uploaded_to_s3_datetime": "2023-10-30 13:38:13.347135" #call_logs state  
    }
    }
    resp=lambda_handler(request, None)