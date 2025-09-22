"""
Author: Aishwarya Chandrasekhar
Version: 1
Date of version: Sep 2025

"""

import copy
from google.cloud import bigquery
import warnings
import category_list as cat
import pytz


warnings.filterwarnings("ignore")


sydney_tz = pytz.timezone("Australia/Sydney")


class IncidentSense():
    def __init__(self):
        ################ GCP INITIALIZE SERVICES ################
        self.bq_client = bigquery.Client()
        self.dataset = "bqai_disaster"
        self.ibtracs_bucket_name_prefix = 'climate_stewardship_ibtracs_data_'
        self.ibtracs_destination_blob_name = "climate_stewardship_ibtracs.csv"

        self.ibtracs_table = "climate_stewardship_ibtracs"
        self.earthquakes_table = "bqai_disaster.earthquakes_noaa_hazel"


        self.NOAA_API_URL = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/earthquakes"

        ################ Query builder helpers ################
        self.cte_others = ''
        self.select_others = ''
        self.join_others = ''
        self.other_selections = ''
        self.other_conditions = ''
        self.other_groupings = ''




    def run_bigquery(self, query):
        '''
        This function runs bigquery queries and returns the row iterator.
        :param query: The query to be run.
        :return: Results of query.
        '''
        job = self.bq_client.query(query)
        print("Done")
        return job.result()


    def upload_table(self, df, dest_table, col=None) -> None:
        """
        The function aims to write the dataframe to a Bigquery table using a schema
        definition and a bigquery client to define the job configuration.
        Arguments: Output Dataframe to be written into the respective table,
        table name, and table ID.
        Returns: None
        """
        df_ = copy.copy(df)
        if col != None:
            df_.insert(0, col, df_.index)
        # Define table name, in format dataset.table_name
        # Load data to BQ
        destination_table = f'{self.default_destination_dataset}.{self.temp_destination_table_prefix}{dest_table}{self.temp_destination_table_suffix}'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.bq_client.load_table_from_dataframe(df_, destination_table, job_config=job_config)
        job.result()
        print(f"{destination_table} created.")
        return destination_table


