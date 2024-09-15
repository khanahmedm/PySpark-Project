import logging
import logging.config
from pyspark.sql.functions import upper, lit

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def perform_data_clean(df1,df2):
    ### Clean df_city DataFrame:
    #1 Select only required Columns
    #2 Convert city, state and county fields to Upper Case
    try:
        logger.info(f"perform_data_clean() is started for df_city dataframe...")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)

    ### Clean df_fact DataFrame:
    #1 Select only required Columns
    #2 Rename the columns
        logger.info(f"perform_data_clean() is started for df_fact dataframe...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),df2.nppes_provider_last_org_name.alias("presc_lname"), \
                             df2.nppes_provider_first_name.alias("presc_fname"),df2.nppes_provider_city.alias("presc_city"), \
                             df2.nppes_provider_state.alias("presc_state"),df2.specialty_description.alias("presc_spclt"), df2.year_exp, \
                             df2.drug_name,df2.total_claim_count.alias("trx_cnt"),df2.total_day_supply, \
                             df2.total_drug_cost)
    #3 Add a Country Field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name",lit("USA"))

    #4 Clean years_of_exp field
    #5 Convert the yearS_of_exp datatype from string to Number
    #6 Combine First Name and Last Name
    #7 Check and clean all the Null/Nan Values
    #8 Impute TRX_CNT where it is null as avg of trx_cnt for the prescriber

    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed...")
    return df_city_sel,df_fact_sel