# S3 File Uploader
## Overview
This repository contains code for an AWS Lambda function designed to download audio from a given URL and upload it to an S3 bucket. It also interfaces with AWS Secrets Manager for managing secret keys and communicates with Snowflake for retrieving and updating data.
The code fetches the URL and other details from a table in Snowflake and then downloads the mp3 audio content and uploads it into AWS s3 bucket. The download and upload process is row by row and has a timeout of 14 mins and 30 sec to prevent the lambda to timeout after 15 mins and perform a graceful exit.
The "table" table is the core table for maintaining the file upload status. Any files left over in the current run will be picked up in the next run for a subsequent upload.


# Deployment
* It deployed as a lambda in the AWS account. 
* manually deployed via Copy/paste into AWS Lambda.


# Local Installation
## Prerequisites

- Python 3.10
- All packages in requirements.txt
- Run gimme-aws-creds. 

## Tasks
1. Clone the GitHub repository to the local directory
2. This code is deployed as a Docker container and need Docker application to support local run.
3. To run this as a docker container, we need to acquire temporary Okta credentials. 
4. For mac machine run the "./runlocal.sh" and for windows machine run "runlocal_windows.bat" to build the docker image and deploy the container running the code.


# Troubleshooting
* Deployment of the container might fail due to AWS credentials. Run gimme-aws-creds.
* For a failure run, check logs in AWS cloudwatch. 