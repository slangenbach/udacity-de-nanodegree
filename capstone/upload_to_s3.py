import logging
import time
from pathlib import Path
from configparser import ConfigParser
import boto3
from botocore.exceptions import ClientError


def create_bucket(bucket_name: str, region: str = 'us-west-2'):
    """
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
    :param bucket_name:
    :param region:
    :return:
    """
    try:
        s3_client = boto3.client('s3', region=region)

        # list buckets
        response = s3_client.list_buckets()

        # check if bucket exists
        if bucket_name not in response['Buckets']:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            logging.warning(f"{bucket_name} already exist in AWS region {region}")

    except ClientError as e:
        logging.exception(e)
        return False

    return True


def upload_file(file_name: str, bucket: str, object_name: str = None, region: str = 'us-west-2'):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    :return:
    """
    if object_name is None:
        object_name = file_name

    try:
        s3_client = boto3.client('s3', region=region)
        s3_client.upload_file(file_name, bucket, object_name)

    except ClientError as e:
        logging.exception(e)
        return False

    return True

if __name__ == '__main__':

    # load config
    config = ConfigParser()
    config.read('app.cfg')

    # start logging
    logging.basicConfig(level=config.get("logging", "level"), format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Started")

    # start timer
    start_time = time.perf_counter()

    # define
    data_path = Path(__file__).parent.joinpath('data')

    # check if bucket exists
    create_bucket(bucket_name='fff-streams')

    # upload files to S3
    upload_file(data_path.joinpath('world_happiness_2017.csv'), bucket='fff-streams', object_name='world_happiness.csv')
    upload_file(data_path.joinpath('temp_by_city_clean.csv'), bucket='fff-streams', object_name='temp_by_city.csv')

    # stop timer
    stop_time = time.perf_counter()
    logging.info(f"Uploaded files in {(stop_time - start_time):.2f} seconds")
    logging.info("Finished")
