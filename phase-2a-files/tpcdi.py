import sys, typer, json, re, logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.functions import *
from typing_extensions import Annotated
from pathlib import Path
from google.cloud import storage

app = typer.Typer(help="A utility for loading TPC-DI generated files into Data Lakehouse.")


def get_session():
    session = SparkSession.builder \
        .appName("TBD-TPC-DI-setup") \
        .enableHiveSupport() \
        .getOrCreate()
    for db in ['digen', 'bronze', 'silver', 'gold']:
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION 'hdfs:///user/hive/warehouse/{db}.db'")
    session.sql('USE digen')
    return session


@app.command(help="Upload a file or files into the stage and build the dependent tables.")
def process_files(
        output_directory: Annotated[
            str, typer.Option(help='The output directory from the TPC-DI DIGen.jar execution.')],
        file_name: Annotated[str, typer.Option(
            help="The TPC-DI file name to upload and process. Pass value 'FINWIRE' to process all of the financial wire files.")] = 'all',
        stage: Annotated[str, typer.Option(help="The stage name to upload to, without specifying '@'.")] = 'tpcdi',
        batch: Annotated[int, typer.Option(
            help="The TPC-DI batch number to process. Currently only supports the default of '1'.")] = 1,
        overwrite: Annotated[bool, typer.Option(help="Overwrite the file even if it exists?")] = False,
        skip_upload: Annotated[bool, typer.Option(help="Skip uploading the files?")] = False,
        show: Annotated[bool, typer.Option(
            help="Show the DataFrame instead of saving it as a table? This was useful during development.")] = False,
):
    session = get_session()

    def get_stage_path(
            stage: str,
            file_name: str,
    ):
        return f"gs://{stage}/tpc-di/{file_name}"

    # method to control printing the dataframe or saving it
    def save_df(
            df: DataFrame,
            table_name: str
    ):
        if show:
            df.show()
        else:
            (
                df.write
                .mode("overwrite")
                .format("parquet")
                .saveAsTable(table_name)
            )

            print(f"{table_name.upper()} table created.")

    # method for uploading files
    def upload_files(
            file_name: str,
            stage_path: str,
    ):

        delimiter = "|"

        if file_name == 'FINWIRE':
            pathlist = Path(output_directory).glob(f"Batch{batch}/FINWIRE??????")
        else:
            pathlist = Path(output_directory).glob(f"Batch{batch}/{file_name}")

        for file in pathlist:
            # capture the delimiter
            if file_name != 'FINWIRE':
                logging.info(f"Suffix: {file.suffix}")
                if file.suffix == '.csv':
                    delimiter = ','
                logging.info(f"Delimiter: {delimiter}")

            # put the file(s) in the stage
            if not skip_upload:
                blob_name = f"tpc-di/{file.name}"
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(stage_path)
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(str(file))
                logging.info(f"Uploaded {str(file)} to {blob_name}")
        # return the delimiter
        return delimiter

    # method for creating a table from a simple CSV
    # works for either comma or pipe delimited, detecting that automatically by suffix
    def load_csv(
            schema: StructType,
            file_name: str,
            table_name: str,
    ):
        stage_path = get_stage_path(stage, file_name)
        delimiter = upload_files(file_name, stage)

        df = (
            session
            .read
            .format("csv")
            .schema(schema)
            .option("delimiter", delimiter)
            .load(stage_path)
        )

        save_df(df, table_name)


    con_file_name = 'Date.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("SK_DATE_ID", IntegerType(), False),
            StructField("DATE_VALUE", DateType(), False),
            StructField("DATE_DESC", StringType(), False),
            StructField("CALENDAR_YEAR_ID", IntegerType(), False),
            StructField("CALENDAR_YEAR_DESC", StringType(), False),
            StructField("CALENDAR_QTR_ID", IntegerType(), False),
            StructField("CALENDAR_QTR_DESC", StringType(), False),
            StructField("CALENDAR_MONTH_ID", IntegerType(), False),
            StructField("CALENDAR_MONTH_DESC", StringType(), False),
            StructField("CALENDAR_WEEK_ID", IntegerType(), False),
            StructField("CALENDAR_WEEK_DESC", StringType(), False),
            StructField("DAY_OF_WEEK_NUM", IntegerType(), False),
            StructField("DAY_OF_WEEK_DESC", StringType(), False),
            StructField("FISCAL_YEAR_ID", IntegerType(), False),
            StructField("FISCAL_YEAR_DESC", StringType(), False),
            StructField("FISCAL_QTR_ID", IntegerType(), False),
            StructField("FISCAL_QTR_DESC", StringType(), False),
            StructField("HOLIDAY_FLAG", BooleanType(), False),
        ])
        load_csv(schema, con_file_name, 'date')

    con_file_name = 'DailyMarket.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("DM_DATE", DateType(), False),
            StructField("DM_S_SYMB", StringType(), False),
            StructField("DM_CLOSE", FloatType(), False),
            StructField("DM_HIGH", FloatType(), False),
            StructField("DM_LOW", FloatType(), False),
            StructField("DM_VOL", FloatType(), False),
        ])
        load_csv(schema, con_file_name, 'daily_market')

    con_file_name = 'Industry.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("IN_ID", StringType(), False),
            StructField("IN_NAME", StringType(), False),
            StructField("IN_SC_ID", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'industry')

    con_file_name = 'Prospect.csv'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("AGENCY_ID", StringType(), False),
            StructField("LAST_NAME", StringType(), True),
            StructField("FIRST_NAME", StringType(), True),
            StructField("MIDDLE_INITIAL", StringType(), True),
            StructField("GENDER", StringType(), True),
            StructField("ADDRESS_LINE1", StringType(), True),
            StructField("ADDRESS_LINE2", StringType(), True),
            StructField("POSTAL_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("INCOME", IntegerType(), True),
            StructField("NUMBER_CARS", IntegerType(), True),
            StructField("NUMBER_CHILDREN", IntegerType(), True),
            StructField("MARITAL_STATUS", StringType(), True),
            StructField("AGE", IntegerType(), True),
            StructField("CREDIT_RATING", IntegerType(), True),
            StructField("OWN_OR_RENT_FLAG", StringType(), True),
            StructField("EMPLOYER", StringType(), True),
            StructField("NUMBER_CREDIT_CARDS", IntegerType(), True),
            StructField("NET_WORTH", IntegerType(), True),
        ])
        load_csv(schema, con_file_name, 'prospect')

    con_file_name = 'CustomerMgmt.xml'
    if file_name in ['all', con_file_name]:
        upload_files(con_file_name, stage)

        # this might get hairy
        df = (
            session
            .read
            .format('xml')
            # .option('STRIP_OUTER_ELEMENT', True)  # Strips the TPCDI:Actions element
            .options(rowTag='TPCDI:Action')
            .load(get_stage_path(stage, con_file_name))
            .select(
                to_timestamp('_ActionTS', 'yyyy-mm-ddThh:mi:ss').alias('action_ts'),
                col('_ActionType').alias('action_type'),
                col('Customer._C_ID').alias('c_id'),
                col('Customer._C_TAX_ID').alias('c_tax_id'),
                col('Customer._C_GNDR').alias('c_gndr'),
                col('Customer._C_TIER').alias('c_tier'),
                col('Customer._C_DOB').alias('c_dob'),
                col('Customer.Name.C_L_NAME'),
                col('Customer.Name.C_F_NAME'),
                col('Customer.Name.C_M_NAME'),
                col('Customer.Address.C_ADLINE1'),
                col('Customer.Address.C_ADLINE2'),
                col('Customer.Address.C_ZIPCODE'),
                col('Customer.Address.C_CITY'),
                col('Customer.Address.C_STATE_PROV'),
                col('Customer.Address.C_CTRY'),
                col('Customer.ContactInfo.C_PRIM_EMAIL'),
                col('Customer.ContactInfo.C_ALT_EMAIL'),
                col('Customer.ContactInfo.C_PHONE_1'),
                col('Customer.ContactInfo.C_PHONE_2'),
                col('Customer.ContactInfo.C_PHONE_3'),
                col('Customer.TaxInfo.C_LCL_TX_ID'),
                col('Customer.TaxInfo.C_NAT_TX_ID'),
                col('Customer.Account._CA_ID').alias('ca_id'),
                col('Customer.Account._CA_TAX_ST').alias('ca_tax_st'),
                col('Customer.Account.CA_B_ID'),
                col('Customer.Account.CA_NAME')
            )

        )
        save_df(df, 'customer_mgmt')

    con_file_name = 'TaxRate.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("TX_ID", StringType(), False),
            StructField("TX_NAME", StringType(), True),
            StructField("TX_RATE", FloatType(), True),
        ])
        load_csv(schema, con_file_name, 'tax_rate')

    con_file_name = 'HR.csv'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("EMPLOYEE_ID", IntegerType(), False),
            StructField("MANAGER_ID", IntegerType(), False),
            StructField("EMPLOYEE_FIRST_NAME", StringType(), True),
            StructField("EMPLOYEE_LAST_NAME", StringType(), True),
            StructField("EMPLOYEE_MI", StringType(), True),
            StructField("EMPLOYEE_JOB_CODE", IntegerType(), True),
            StructField("EMPLOYEE_BRANCH", StringType(), True),
            StructField("EMPLOYEE_OFFICE", StringType(), True),
            StructField("EMPLOYEE_PHONE", StringType(), True)
        ])
        load_csv(schema, con_file_name, 'hr')

    con_file_name = 'WatchHistory.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("W_C_ID", IntegerType(), False),
            StructField("W_S_SYMB", StringType(), True),
            StructField("W_DTS", TimestampType(), True),
            StructField("W_ACTION", StringType(), True)
        ])
        load_csv(schema, con_file_name, 'watch_history')

    con_file_name = 'Trade.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("T_ID", IntegerType(), False),
            StructField("T_DTS", TimestampType(), False),
            StructField("T_ST_ID", StringType(), False),
            StructField("T_TT_ID", StringType(), False),
            StructField("T_IS_CASH", BooleanType(), False),
            StructField("T_S_SYMB", StringType(), False),
            StructField("T_QTY", FloatType(), False),
            StructField("T_BID_PRICE", FloatType(), False),
            StructField("T_CA_ID", IntegerType(), False),
            StructField("T_EXEC_NAME", StringType(), False),
            StructField("T_TRADE_PRICE", FloatType(), True),
            StructField("T_CHRG", FloatType(), True),
            StructField("T_COMM", FloatType(), True),
            StructField("T_TAX", FloatType(), True),
        ])
        load_csv(schema, con_file_name, 'trade')

    con_file_name = 'TradeHistory.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("TH_T_ID", IntegerType(), False),
            StructField("TH_DTS", TimestampType(), False),
            StructField("TH_ST_ID", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'trade_history')

    con_file_name = 'StatusType.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("ST_ID", StringType(), False),
            StructField("ST_NAME", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'status_type')

    con_file_name = 'TradeType.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("TT_ID", StringType(), False),
            StructField("TT_NAME", StringType(), False),
            StructField("TT_IS_SELL", BooleanType(), False),
            StructField("TT_IS_MARKET", BooleanType(), False),
        ])
        load_csv(schema, con_file_name, 'trade_type')

    con_file_name = 'HoldingHistory.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("HH_H_T_ID", IntegerType(), False),
            StructField("HH_T_ID", IntegerType(), False),
            StructField("HH_BEFORE_QTY", FloatType(), False),
            StructField("HH_AFTER_QTY", FloatType(), False),
        ])
        load_csv(schema, con_file_name, 'holding_history')

    con_file_name = 'CashTransaction.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
            StructField("CT_CA_ID", IntegerType(), False),
            StructField("CT_DTS", TimestampType(), False),
            StructField("CT_AMT", FloatType(), False),
            StructField("CT_NAME", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'cash_transaction')

    con_file_name = 'FINWIRE'
    if file_name in ['all', con_file_name]:
        # These are fixed-width fields, so read the entire line in as "line"
        schema = StructType([
            StructField("line", StringType(), False),
        ])

        stage_path = get_stage_path(stage, f"{con_file_name}*")
        upload_files(con_file_name, stage)

        # generic dataframe for all record types
        # create a temporary table
        df = (
            session
            .read
            .schema(schema)
            .option('delimiter', '|')
            .csv(stage_path)
            .withColumn('rec_type', substring('line', 16, 3))
            .withColumn('pts', to_timestamp(substring('line', 0, 15), 'yyyyMMdd-HHmmss')) #20101231-100058
            .createOrReplaceTempView("finwire")
        )

        # CMP record types
        df = (
            session
            .table('finwire')
            .where(col('rec_type') == 'CMP')
            .withColumn('company_name', substring('line', 19, 60))
            .withColumn('cik', substring('line', 79, 10))
            .withColumn('status', substring('line', 89, 4))
            .withColumn('industry_id', substring('line', 93, 2))
            .withColumn('sp_rating', substring('line', 95, 4))
            .withColumn(
                'founding_date',
                to_date(trim(
                    substring(
                        'line', 99, 8)
                ),
                    'yyyyMMdd')
            )
            .withColumn('address_line1', substring('line', 107, 80))
            .withColumn('address_line2', substring('line', 187, 80))
            .withColumn('postal_code', substring('line', 267, 12))
            .withColumn('city', substring('line', 279, 25))
            .withColumn('state_province', substring('line', 304, 20))
            .withColumn('country', substring('line', 324, 24))
            .withColumn('ceo_name', substring('line', 348, 46))
            .withColumn('description', substring('line', 394, 150))
            .drop('line', 'rec_type')
        )

        save_df(df, 'cmp')

        # SEC record types
        df = (
            session
            .table('finwire')
            .where(col('rec_type') == 'SEC')
            .withColumn('symbol', substring('line', 19, 15))
            .withColumn('issue_type', substring('line', 34, 6))
            .withColumn('status', substring('line', 40, 4))
            .withColumn('name', substring('line', 44, 70))
            .withColumn('ex_id', substring('line', 114, 6))
            .withColumn('sh_out', substring('line', 120, 13))
            .withColumn('first_trade_date', substring('line', 133, 8))
            .withColumn('first_exchange_date', substring('line', 141, 8))
            .withColumn('dividend', substring('line', 149, 12))
            .withColumn('co_name_or_cik', substring('line', 161, 60))
            .drop('line', 'rec_type')
        )

        save_df(df, 'sec')

        # FIN record types
        df = (
            session
            .table('finwire')
            .where(col('rec_type') == 'FIN')
            .withColumn('year', substring('line', 19, 4))
            .withColumn('quarter', substring('line', 23, 1))
            .withColumn('quarter_start_date', substring('line', 24, 8))
            .withColumn('posting_date', substring('line', 32, 8))
            .withColumn('revenue', substring('line', 40, 17))
            .withColumn('earnings', substring('line', 57, 17))
            .withColumn('eps', substring('line', 74, 12))
            .withColumn('diluted_eps', substring('line', 86, 12))
            .withColumn('margin', substring('line', 98, 12))
            .withColumn('inventory', substring('line', 110, 17))
            .withColumn('assets', substring('line', 127, 17))
            .withColumn('liabilities', substring('line', 144, 17))
            .withColumn('sh_out', substring('line', 161, 13))
            .withColumn('diluted_sh_out', substring('line', 174, 13))
            .withColumn('co_name_or_cik', substring('line', 187, 60))
            .drop('line', 'rec_type')
        )

        save_df(df, 'fin')


if __name__ == "__main__":
    app()
