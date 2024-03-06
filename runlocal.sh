docker build -t s3-file-uploader -f Dockerfile .
docker run -v ~/.aws/credentials:/root/.aws/credentials s3-file-uploader