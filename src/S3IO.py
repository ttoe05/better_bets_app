"""
This file is used to interact with S3. In order to use it, standard credentials and configuration files
will need to be set up. You can follow the below resources on how to set it up.

resources:

1. https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
2. https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
"""

import logging
import boto3
import tempfile
import json
import pandas as pd
from io import BytesIO


def retry(times):
    def decorator(func):
        def aux(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.info(f"On attempt {attempt}")
                    logging.info(e)
            return func(*args, **kwargs)
        return aux
    return decorator


class S3IO():
    """
    Wrapper class to the boto3 package for easy interaction with S3. Premise is to
    give data scientists and data engineers tools to enhance their workflows
    """
    # initialization
    def __init__(self,
                 bucket: str,
                 profile: str = 'default'):
        """
        Initialization of the S3IO object.

        Parameters
        ----------
        bucket: str
            The name of the AWS bucket

        profile: str
            The name of the profile that holds the access key
            and access id in the AWS credentials file
        """
        self.bucket = bucket
        self._profile = profile
        # establish a connection with s3
        logging.info("Establishing a connection with S3 using passed parameters")
        try:
            session = boto3.Session(profile_name=self._profile)
        except Exception as e:
            logging.error(f"{e}\nCheck spelling of profile or properly set it in credentials file")
            raise ValueError
        self.s3_client = session.client('s3')
        self.s3_resource = session.resource('s3')


    def s3_is_dir(self,
                  path: str) -> bool:
        """
        Function checks if a given path exists in s3

        Parameters
        ----------
        path: str
            s3 path being verified

        Returns
        -------
        bool:   True if the path exists and False if not
        """
        objs = list(self.s3_resource.Bucket(
            self.bucket).objects.filter(Prefix=path))
        return len(objs) > 1 or (len(objs) == 1 and objs[0].key != path)


    def s3_list_obj(self,
                    path: str) -> list:
        """
        Function returns the list of objects in a given path for the
        bucket that was initialized.

        Parameters
        ----------
        path: str
            The path where objects are stored

        Returns
        -------
            list:   the list of objects that exist in the path that was passed.
                    If the path is a directory that does not exist in the bucket,
                    the function will return an empty list.
        """
        # Retrieve the objects in the passed path
        all_obj = self.s3_resource.Bucket(self.bucket).objects.filter(Prefix=path)
        # Parse dictionary to retrieve the objects
        result = [x.key for x in all_obj]
        # Check if the reusults is an empty list
        if not result:
            logging.warning((f"Results produced an empty list for path: {path} "
                             f"Check the spelling of the path or its existence"))
        return result

    def s3_read_xlsx(self,
                     file_path: str,
                     sheet_name: str) -> pd.DataFrame:
        """
        Function takes the full path of a xlsx file as input and outputs
        a pandas dataframe of the file.

        Parameters
        ----------
        file_path: str
            The full path of the file, example -> file/located/here.csv

        sheet_name: str
            The name of the sheet in the excel file to loaded

        Returns
        -------
        pd.DataFrame:   Dataframe of the file content
        """
        # Get object from s3
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
        data = pd.read_excel(obj['Body'].read(), sheet_name=sheet_name)
        return data

    @retry(times=5)
    def s3_read_csv(self,
                    file_path: str) -> pd.DataFrame:
        """
        Function takes the full path of a csv file as input and outputs
        a pandas dataframe of the file.

        Parameters
        ----------
        file_path: str
            The full path of the file, example -> file/located/here.csv

        Returns
        -------
        pd.DataFrame:   Dataframe of the file content
        """
        # Get object from s3
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
        data = pd.read_csv(obj['Body'])
        return data

    @retry(times=5)
    def s3_write_csv(self,
                     df: pd.DataFrame,
                     file_path: str,
                     index: bool = False) -> None:
        """
        Function takes a local pandas dataframe and writes it to a specified
        path as a csv file

        Parameters
        ----------
        df: pd.DataFrame
            local pandas dataframe

        file_path: str
            the full file path where the dataframe will be saved in s3
            example ->  file/located/here.csv

        index: bool
            whether to save the dataframe indexes as part of the csv file
        """
        # check if the input is a dataframe
        if not isinstance(df, pd.DataFrame):
            logging.error("Input is not of type pd.DataFrame")
            raise ValueError("Please pass a Pandas DataFrame")
        # Check if the file extension is csv
        extension = file_path.split(".")[-1]
        if extension != "csv":
            logging.error("File extension should be csv")
            raise ValueError("Please pass a file name with the extension csv")
        # Check if the path for the file exists
        split_path = file_path.split("/")[:-1]
        path = "/".join(split_path)
        if not self.s3_is_dir(path):
            logging.error(f"The given path does not exist: {path}")
            raise ValueError("Please pass a valid path")
        # Create a temp file locally using df and load to the path s3
        with tempfile.NamedTemporaryFile(delete=True, mode='r+') as temp:
            df.to_csv(temp.name + ".csv", index=index,)
            self.s3_resource.Bucket(self.bucket).upload_file(
                temp.name + ".csv", Key=file_path)

    @retry(times=5)
    def load_json(self, data: dict, file_path: str) -> None:
        """
        The function takes a dictionary object of data and persists the object to an s3 bucket
        as a json file.

        Parameters
        _______________
            data: dict
        the dictionay object that will be perisisted as json file

            file_path
        the file path where the json object will be persisted in s3
        """

        logging.info("Writing dict data to s3")
        json_object = json.dumps(data, indent=4)
        self.s3_resource.Object(self.bucket, file_path).put(Body=json_object)

    @retry(times=5)
    def read_json(self, file_path:str) -> dict:
        """
        Function reads in a json file persisted in s3
        Parameters
        ___________
            file_path: str
        The file path where the json file is persisted

        Return
        ___________
            dict
        """

        obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
        data = json.load(obj['Body'])
        return data

    @retry(times=5)
    def s3_write_parquet(self,
                         df: pd.DataFrame,
                         file_path: str) -> None:
        """
        Function takes a local pandas dataframe and writes it to a specified
        path as a parquet file

        Parameters
        ----------
        df: pd.DataFrame
            local pandas dataframe

        file_path: str
            the full file path where the dataframe will be saved in s3
            example ->  file/located/here.parq
        """
        # Create a temp file locally using df and load to the path s3
        # with tempfile.NamedTemporaryFile(delete=True, mode='r+') as temp:
        #     df.to_parquet(temp.name + ".parq")
        #     self.s3_resource.Bucket(self.bucket).upload_file(
        #         temp.name + ".parq", Key=file_path)
        buffer = BytesIO()
        df.to_parquet(buffer)
        self.s3_resource.Object(self.bucket, file_path).put(Body=buffer.getvalue())

    @retry(times=5)
    def s3_read_parquet(self,
                        file_path: str) -> pd.DataFrame:
        """
        Function takes the full path of a parquet file as input and outputs
        a pandas dataframe of the file.

        Parameters
        ----------
        file_path: str
            The full path of the file, example -> file/located/here.parq

        Returns
        -------
        pd.DataFrame:   Dataframe of the file content
        """
        # Get object from s3
        # obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
        # data = pd.read_parquet(obj['Body'].read())
        buffer = BytesIO()
        file_object = self.s3_resource.Object(self.bucket, file_path)
        file_object.download_fileobj(buffer)
        data = pd.read_parquet(buffer)
        return data
