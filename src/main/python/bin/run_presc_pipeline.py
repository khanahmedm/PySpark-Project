### Import all the necessary Modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec
import sys
import logging
import logging.config
import os
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean

### Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
        logging.info("main() is started ...")
        ### Get Spark Object
        spark = get_spark_object(gav.envn,gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        ### Initiate presc_run_data_ingest Script
        # Load the City File
        for file in os.listdir(gav.staging_dim_city):
            print("File is "+ file)
            file_dir = gav.staging_dim_city + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv' :
                file_format = 'csv'
                header=gav.header
                inferSchema=gav.inferSchema
            elif file.split('.')[1] == 'parquet' :
                file_format = 'parquet'
                header='NA'
                inferSchema='NA'

        df_city = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)

        # Load the Prescriber Fact File
        for file in os.listdir(gav.staging_fact):
            print("File is "+ file)
            file_dir = gav.staging_fact + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv' :
                file_format = 'csv'
                header=gav.header
                inferSchema=gav.inferSchema
            elif file.split('.')[1] == 'parquet' :
                file_format = 'parquet'
                header='NA'
                inferSchema='NA'
        df_fact = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)

        ### Validate run_data_ingest script for city Dimension & Prescriber Fact dataframe
        df_count(df_city,'df_city')
        df_top10_rec(df_city,'df_city')

        df_count(df_fact,'df_fact')
        df_top10_rec(df_fact,'df_fact')

        ### Initiate presc_run_data_preprocessing Script
        ## Perform data Cleaning Operations for df_city
        df_city_sel = perform_data_clean(df_city)

        #Validation for df_city
        df_top10_rec(df_city_sel,'df_city_sel')


        ### Initiate run_presc_data_transform Script
        # Apply all the transfrmations Logics
        # Validate

        # Set up Logging Configuration Mechanism

        ### Initiate run_data_extraction Script
        # Validate
        # Set up Error Handling
        # Set up Logging Configuration Mechanism

        ### End of Application Part 1
        logging.info("presc_run_pipeline.py is Completed.")

    except Exception as exp:
        logging.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
              "and fix it." +str(exp),exc_info=True)
        sys.exit(1)

if __name__ == "__main__" :
    logging.info("run_presc_pipeline is Started ...")
    main()