import os
from datetime import date, timedelta, datetime
import pandas as pd
from google.cloud import bigquery as bq
from google.api_core.exceptions import BadRequest
from loguru import logger


class Load:
    def __init__(self, amo, entity):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
            './tokens/oddjob-db-2007-759fe782b144.json'
        self.client = bq.Client()
        self.path = f"temp_data/{amo}/{entity}_transformed/"
        self.date = str(date.today())
        self.yesterday = str(date.today() - timedelta(days=1))
        self.files_num = sum(1 for file in os.listdir(self.path))
        self.file_list = [self.path + f for f in os.listdir(self.path)]

        self.table_ref = self.client.dataset(
                f"{amo}_oddjob").table(f"dw_amocrm_{entity}")

        self.table_backup = self.client.dataset(
            f"{amo}_oddjob").table(f"dw_amocrm_{entity}_{self.date}")
        self.table_backup_old = self.client.dataset(
            f"{amo}_oddjob").table(f"dw_amocrm_{entity}_{self.yesterday}")

        self.job_config = bq.LoadJobConfig(autodetect=True)


    def backup(self):
        job = self.client.copy_table(
            self.table_ref, self.table_backup
        )
        logger.success("Table successfully backed up!")
        self.client.delete_table(self.table_ref)
        self.client.delete_table(self.table_backup_old)
        self.client.create_table(self.table_ref)


    def read(self, filepath):
        return pd.read_csv(filepath)


    def load(self, df):
        job = self.client.load_table_from_dataframe(
            df, self.table_ref, job_config=self.job_config
        )
        logger.success(job.result())


    def in_batches(self, batch_size=10000):
        logger.info(f"There are {self.files_num} files to send...")
        for i in range(0, self.files_num, batch_size):
            batch_files = self.file_list[i:i+batch_size]
            df_list = [self.read(f) for f in batch_files]
            df = pd.concat(df_list).astype('str')
            self.load(df)
        logger.success("Load successful!")
        return True

    def cleanup(self):
        for f in self.file_list:
            os.remove(f)

class LoadWithSchemaUpdate(Load):
    def __init__(self, amo, entity):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
            './tokens/oddjob-db-2007-759fe782b144.json'
        self.client = bq.Client()
        self.path = f"temp_data/{amo}/{entity}_transformed/"
        self.date = str(datetime.now()).replace(" ", "-")\
                                       .replace(":", "-")\
                                       .split(".")[0]
        self.files_num = sum(1 for file in os.listdir(self.path))
        self.file_list = [self.path + f for f in os.listdir(self.path)]

        self.table_ref = self.client.dataset(
                f"{amo}_oddjob").table(f"dw_amocrm_{entity}")

        self.table_backup = self.client.dataset(
            f"{amo}_oddjob").table(f"dw_amocrm_{entity}_{self.date}")
        self.job_config = bq.LoadJobConfig(autodetect=True)


    def get_schema_from_dataframe(self, df, old_schema):
        df[df.select_dtypes(include=['object']).columns] =\
            df.select_dtypes(include=['object']).astype('string')
        old_schema_names = [field.name for field in old_schema]
        schema = []

        for col_name, dtype in df.dtypes.items():
            if col_name not in old_schema_names:
                schema.append(bq.SchemaField(col_name, dtype.name))
        return schema


    def update_schema(self, df):
        table = self.client.get_table(self.table_ref)

        old_schema = list(table.schema)
        new_schema = self.get_schema_from_dataframe(df, old_schema)

        combined_schema = old_schema + new_schema

        table.schema = combined_schema
        self.client.update_table(table, ["schema"])


    def in_batches(self, batch_size=10000):
        for i in range(0, self.files_num, batch_size):
            batch_files = self.file_list[i:i+batch_size]
            df_list = [self.read(f) for f in batch_files]
            df = pd.concat(df_list).astype('str')
            try:
                self.load(df)

            except BadRequest as e:
                logger.info(e)
                self.update_schema(df)
                self.load(df)
        return True
