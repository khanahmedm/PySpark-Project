import logging
import logging.config
from pyspark.sql.functions import upper

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def perform_data_clean(df1):
    ### Clean df_city DataFrame:
    #1 Select only required Columns
    #2 Convert city, state and county fields to Upper Case
    try:
        logger.info(f"perform_data_clean() is started ...")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed...")
    return df_city_sel