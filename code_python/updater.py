import pandas as pd
import numpy as np
import urllib.request as urlreq
import pymysql
import yaml
from datetime import timedelta, datetime as dt
import time, json
import logging
from collections import OrderedDict
import os
import sys


# YAML file containing database login credentials (renamed from creds_template.yaml)
cred_file = "./creds.yaml"
if not os.path.exists(cred_file):
    print("creds.yaml not found: did you rename creds_template.yaml?")
    sys.exit()


def init_logger():
    logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger()
    return logger


#################################
##### SQL Utility Functions #####
#################################


def mysql_connect(host, user, password, database):
    # if using mysql-connector-python (causes problems with pd.read_sql_query for gui).
    # conn = mysql.connector.connect(host=server, user=user, password=password, database=database, use_pure=True)
    conn = pymysql.connect(host=host, user=user, password=password, db=database)
    cursor = conn.cursor()
    return conn, cursor


def fetchone(sql_cursor, query):
    """ Calls fetchone() on a query. If the result is empty, the function returns None, otherwise returns the first
    element of the return value tuple.

    :param sql_cursor:
    :param query:
    :return: None or the first element of the return value tuple.
    """
    sql_cursor.execute(query)
    answer = None
    tmp = sql_cursor.fetchone()
    if tmp is not None:
        answer = tmp[0]
    return answer


def npdt2str(a_dt):
    # Convert numpy datetime64 to a string compatible with MySQL datetime
    return pd.to_datetime(str(a_dt)).strftime("%Y-%m-%d %H:%M:%S")


###########################
##### Ticker Grabbers #####
###########################


def get_alphavantage_tickers():
    source_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    data = pd.read_csv(source_url, header=0, index_col=0)

    tickers = OrderedDict()
    for ticker in sorted(data.index):
        if ticker in ["BF.B"]:  # skip list
            continue
        tickers[ticker] = "AlphaVantage"

    return tickers


def get_dow30_stocks(source_url="https://www.cnbc.com/dow-components/"):
    data = pd.read_html(source_url)
    return sorted(list(data[0]["Symbol"]))


def get_nasdaq100_stocks(source_url="https://www.cnbc.com/nasdaq-100/"):
    data = pd.read_html(source_url)
    return sorted(list(data[0]["Symbol"]))


def get_sp500_stocks(source_url="https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"):
    """ Pull S&P500 consituent tickers from web-based CSV.

    :param source_url: web location of CSV file containing the S&P500 consituent tickers.
    :return: list of tickers
    """
    data = pd.read_csv(source_url, header=0, index_col=0)
    return sorted(list(data.index))


def get_etf_tickers(inc_stocks=True, inc_bonds=True, inc_commods=True, inc_fx=True):
    stocks = ["QQQ", "SPY", "IWM", "IVW", "IWB"]
    bonds = ["HYG", "JNK", "TBX", "AGG", "BND", "VNQ"]
    commods = ["USO"]
    fx = ["UUP"]
    tickers = inc_stocks * stocks + inc_bonds * bonds + inc_commods * commods + inc_fx * fx
    tickers = {i: "AlphaVantage" for i in sorted(tickers)}
    return tickers


#######################################
##### Reporting Support Functions #####
#######################################


def get_price_relative_to_avg(sql_conn, above_or_below, length=30):
    # With mysql-python-connector, pd.read_sql_query() fails, so this can be a workaround but the
    # headers don't get returns properly. Cursor.description is supposed to show the headers, but
    # they aren't as expected for some reason.

    # sql_cursor.callproc("GetSecurities{}Avg".format(above_or_below), args=[length])
    # for i in sql_cursor.stored_results():
    #     results = i.fetchall()
    # data = pd.DataFrame(results)

    # pd.read_sql_query() works with pymssql though.
    sqlstr = "CALL GetSecurities{}Avg({})".format(above_or_below, length)
    data = pd.read_sql_query(sql=sqlstr, con=sql_conn)
    return data


def get_range_summary(sql_conn, start_date=dt.now()-timedelta(days=365), end_date=dt.now()):
    sqlstr = "CALL GetPriceRangeOverDateRange('{}', '{}');".format(
        start_date.strftime("%m/%d/%y"),
        end_date.strftime("%m/%d/%y"))
    data = pd.read_sql_query(sql=sqlstr, con=sql_conn)
    return data


def get_highest_lowest_close(sql_conn, start_date=dt.now()-timedelta(days=365), end_date=dt.now()):
    sqlstr = "CALL GetTrailingHighestAndLowestClose('{}', '{}');".format(
        start_date.strftime("%m/%d/%y"),
        end_date.strftime("%m/%d/%y"))
    data = pd.read_sql_query(sql=sqlstr, con=sql_conn)
    return data


#########################################
##### Data Source Support Functions #####
#########################################


def get_alphavantage_id(sql_cursor):
    data_source_id = fetchone(sql_cursor, query = "SELECT GetDataSourceIDFromDataSourceName('AlphaVantage')")
    return data_source_id


def load_data_from_alphavantage(logger, ticker, api_key, outputsize="full", intraday=False, interval='5min'):
    """ Pull JSON-formatted Security price data from AlphaVantage.

    :param ticker: the Security symbol.
    :param api_key: user's API key.
    :param outputsize: "full" or "compact"
    :param intraday: bool, if True, grab intraday data, otherwise grab daily data.
    :param interval: intraday sample frequency in minutes. Only valid when intraday=True.
    :return: a Pandas dataframe containing the price information.
    """
    logger("... getting data from AlphaVantage: {}".format(ticker))

    base_url = "https://www.alphavantage.co/query?function="
    if intraday:
        base_url = "{}TIME_SERIES_INTRADAY&".format(base_url)
        option_url = "symbol={}&interval={}&outputsize={}&apikey={}".format(ticker, interval, outputsize, api_key)
    else:
        base_url = "{}TIME_SERIES_DAILY_ADJUSTED&".format(base_url)
        option_url = "symbol={}&outputsize={}&apikey={}".format(ticker, outputsize, api_key)

    data = json.loads(urlreq.urlopen(base_url + option_url).read().decode())

    ts_data = data[[i for i in data.keys() if 'Time Series' in i][0]]

    ts = pd.DataFrame(data=list(ts_data.values()), index=list(ts_data.keys()))
    ts.columns = [i.split(".")[-1].strip() for i in ts.columns]
    ts.index = pd.to_datetime(ts.index)
    ts.sort_index(inplace=True)

    if intraday:
        ts['adjusted close'] = None
        ts['dividend amount'] = None
        ts['split coefficient'] = None

    # make sure column order is consistent with what our SQL insert expects
    columns = ["open", "high", "low", "close", "adjusted close", "volume", "dividend amount", "split coefficient"]
    ts = ts[columns]

    return ts


#####################################
##### Database Update Functions #####
#####################################


def database_update(
        logger,
        sql_conn,
        sql_cursor,
        data_source_info,
        wait_seconds=2,
        cutoff_hour=17,
        tickers=[]):
    """ Main routine to seed and/or update Security price database.

    :param sql_conn: active SQL server connection.
    :param sql_cursor: a SQL cursor from an active connection.
    :param logger: a logging instance, e.g. logger.info or textEdit.append
    :param cutoff_hour: only collect today's data if the current hour is > cutoff hour.
    :param data_source_name: AlphaVantage.
    :param data_source_url: https://www.alphavantage.co
    :return: no return value.
    """

    if not tickers:
        tickers = get_symbols_from_database(sql_conn)  # or get_dow30_stocks() or ...

    start = time.time()
    ticker_list = list(tickers.keys())

    # If items are removed from the ticker lists, drop them from the database.
    to_drop = list(set(get_symbols_from_database(sql_conn)) - set(ticker_list))
    if to_drop:
        logger("Symbols removed from ticker list that will be deleted: {}".format(" ,".join(to_drop)))
        [delete_existing_symbol(symbol, sql_conn=sql_conn, sql_cursor=sql_cursor, logger=logger) for symbol in to_drop]

    dtnow = dt.now()
    if dtnow.hour < cutoff_hour or dtnow.weekday() > 4:
        update_through_date = dtnow - pd.tseries.offsets.BDay(n=1)
    else:
        update_through_date = dt(dtnow.year, dtnow.month, dtnow.day, hour=23, minute=59)

    while ticker_list:
        ticker = ticker_list.pop(0)
        data_source_name = tickers[ticker]

        data_source_id = fetchone(sql_cursor, query="SELECT GetDataSourceIDFromDataSourceName('{}')".format(data_source_name))

        if not data_source_id:
            data_source_id = update_data_source(
                logger=logger,
                sql_conn=sql_conn,
                sql_cursor=sql_cursor,
                data_source_name=data_source_name,
                data_source_url=data_source_info[data_source_name]['url'])

        logger("Processing {} ({} remaining)".format(ticker, len(ticker_list)))

        try:

            ########################################
            # Update SecurityMetaData if necessary #
            ########################################

            security_metadata_id = fetchone(sql_cursor, query="SELECT GetMetaDataIDForSymbol('{}')".format(ticker))

            if not security_metadata_id:
                update_metadata(
                    logger=logger,
                    sql_conn=sql_conn,
                    sql_cursor=sql_cursor,
                    ticker=ticker,
                    data_source_id=data_source_id)

            #################################################
            # Update SecurityPriceObservations if necessary #
            #################################################

            last_dt_in_db = fetchone(sql_cursor, query="SELECT GetLastSampleTimeForSecurity('{}')".format(ticker))

            if (last_dt_in_db is not None) and (last_dt_in_db.date() == update_through_date.date()):
                logger("... no update required: record is up to date.")
                continue

            if last_dt_in_db is None:
                # Happens if new symbol is added but no data exists.
                logger("... seeding new data through {}".format(update_through_date.date()))
                seed_mode = True
            else:
                seed_mode = False

            update_price_observations(
                logger=logger,
                sql_conn=sql_conn,
                sql_cursor=sql_cursor,
                ticker=ticker,
                data_source_name=data_source_name,
                data_source_id=data_source_id,
                data_source_info=data_source_info,
                last_dt_in_db=last_dt_in_db,
                update_through_date=update_through_date,
                seed_mode=seed_mode)

            if wait_seconds > 0 and len(ticker_list) > 0:
                logger("... waiting {} seconds so we don't spam the data provider's server.".format(wait_seconds))
                time.sleep(wait_seconds)

        except:
            logger("... FAILED!")
            ticker_list.append(ticker)
            logger("... waiting {} seconds so we don't spam the data provider's server.".format(wait_seconds))
            time.sleep(wait_seconds)

    end = time.time()
    logger("Processing took {:.2f} minutes".format((end - start) / 60.0))


def update_data_source(logger, sql_conn, sql_cursor, data_source_name, data_source_url):
    """ Insert new data source info into DataSource table, if it doesn't already exist.

    :param logger: logger: a logging instance, e.g. logger.info or textEdit.append
    :param sql_conn: active SQL server connection.
    :param sql_cursor: a SQL cursor from an active connection.
    :param data_source_name: name of new data source.
    :param data_source_url: URL for new data source.
    :return: ID of data source in database.
    """
    data_source_id = fetchone(sql_cursor, query="SELECT GetDataSourceIDFromDataSourceName('{}')".format(data_source_name))

    if not data_source_id:
        logger("Adding new data source:")
        logger("... name: {}".format(data_source_name))
        logger("... URL: {}".format(data_source_url))

        sql_cursor.callproc('AddNewDataSource', args=[data_source_name, data_source_url])
        sql_conn.commit()

        data_source_id = fetchone(sql_cursor, query="SELECT GetDataSourceIDFromDataSourceName('{}')".format(data_source_name))
        logger("... DataSourceID={}".format(data_source_id))
    else:
        logger("{} is already defined with DataSourceID={}".format(
            data_source_name, data_source_id))

    return data_source_id


def  update_metadata(
        logger,
        sql_conn,
        sql_cursor,
        ticker,
        data_source_id,
        sec_type='EQUITY',
        sec_tz='EST',
        sec_contract_size=1.0,
        sec_currency='USD'):
    """ SQL code to update the SecurityMetaData table.

    :param logger: a logging instance, e.g. logger.info or textEdit.append
    :param sql_conn: active SQL server connection.
    :param sql_cursor: a SQL cursor from an active connection.
    :param data_source_id: the ID to use as the foreign key.
    :param ticker: the ticker symbol to process.
    :param sec_type: security type, default EQUITY.
    :param sec_tz: traded time zones, default EST.
    :param sec_contract_size: security contract size, default 1.
    :param sec_currency: security denomination currency, default USD.
    :return: the table's primary key (which can be used elsewhere as a foreign key).
    """
    security_metadata_id = fetchone(sql_cursor, query="SELECT GetMetaDataIDForSymbol('{}')".format(ticker))

    if not security_metadata_id:
        logger("Adding metadata for new symbol:")
        logger("... symbol: {}".format(ticker))
        logger("... type: {}".format(sec_type))
        logger("... timezone: {}".format(sec_tz))
        logger("... contract size: {}".format(sec_contract_size))
        logger("... currency: {}".format(sec_currency))
        logger("... DataSourceID: {}".format(data_source_id))

        sql_cursor.callproc('AddNewSymbol', args=[
            ticker, sec_type, sec_tz, sec_contract_size, sec_currency, data_source_id])
        sql_conn.commit()
    else:
        logger("{} is already defined with SecurityMetaDataID={}".format(ticker, data_source_id))


def update_price_observations(
        logger,
        sql_conn,
        sql_cursor,
        ticker,
        data_source_name,
        data_source_id,
        data_source_info,
        last_dt_in_db,
        update_through_date,
        seed_mode=False):
    """

    :param logger: a logging instance, e.g. logger.info or textEdit.append
    :param sql_conn: active SQL server connection.
    :param sql_cursor: a SQL cursor from an active connection.
    :param ticker: the ticker symbol to process.
    :param data_source_name: the name of the datasource associated with the ticker.
    :param data_source_id: primary key associated with the data source name (pulled from database).
    :param data_source_info: dict of datasource info paraed from cred file.
    :param update_through_date: date after whuch we should trim raw data.
    :param seed_mode: if True, pull "full" data from source, otherwise pull "compact" data.
    :return: no return value.
    """

    # It seems that "full" pulls tend to have an end date that is a few days old, while
    # "compact" pulls go through the current date. So an initial seed tends to be missing
    # a few recent days, and thus takes a second update pass to pick up the remaining data.

    if data_source_name == "AlphaVantage":
        http_call = {True: "full", False: "compact"}
        raw_data = load_data_from_alphavantage(
            logger=logger,
            ticker=ticker,
            api_key=data_source_info[data_source_name]['api_key'],
            outputsize=http_call[seed_mode],
            intraday="intraday" in str(sql_conn.db).lower())
        # isolate the data to update
        if last_dt_in_db is not None:
            raw_data = raw_data[raw_data.index > last_dt_in_db]
        raw_data = raw_data[:update_through_date]  # trim
    else:
        logger("Datasource not supported: {}".format(data_source_name))

    # replace NaN with None for SQL compatibility
    raw_data = raw_data.where(raw_data.notnull(), None)

    raw_data["data_source_id"] = str(data_source_id)
    raw_data["ticker"] = ticker

    if not raw_data.empty:
        logger('... inserting into table: DataSourcePriceObservation')
        stmt = """
            INSERT INTO
            DataSourcePriceObservation (
                SampleTime,
                OpenPrice,
                HighPrice,
                LowPrice,
                ClosePrice,
                AdjustedClosePrice,
                Volume,
                DividendAmount,
                SplitCoefficient,
                DataSourceID,
                SecurityMetaDataID)
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                GetMetaDataIDForSymbol(%s));
            """

        tuple_data = [tuple(x) for x in raw_data.to_records(index=True)]

        # convert numpy datetime64 to a date string that MySQL can handle
        for i in range(len(tuple_data)):
            tuple_data[i] = tuple((npdt2str(x) if type(x) == np.datetime64 else x for x in tuple_data[i]))

        sql_cursor.executemany(stmt, tuple_data)
        sql_conn.commit()


##########################################
##### Database Maintenance Functions #####
##########################################


def delete_existing_symbol(symbol, sql_conn, sql_cursor, logger):
    """ Remove a security and all of its data from the database.

    :param symbol: the ticker symbol to process.
    :param sql_conn: active SQL server connection.
    :param sql_cursor: a SQL cursor from an active connection.
    :param logger: a logging instance, e.g. logger.info or textEdit.append
    :return: no return value.
    """
    answer = fetchone(
        sql_cursor, 
        query="SELECT SecurityMetaDataID FROM SecurityMetaData WHERE SecuritySymbol='{}'".format(symbol))

    if answer:
        logger('Deleting {} and all associated data.'.format(symbol))
        sql_cursor.callproc('DeleteSymbol', args=[symbol])
        logger("... done.")
        sql_conn.commit()
    else:
        logger("{} not found in database.".format(symbol))


# def get_security_data_from_database(ticker):
#     """ Get time series data from SQL server.
#
#     :param ticker: security to get data for.
#     :return:
#     """
#     sql_conn, _ = mysql_connect()
#     sqlstr = "CALL GetSecurityData('{}')".format(ticker)
#     data = pd.read_sql_query(sql=sqlstr, con=sql_conn, index_col='SampleTime')
#     sql_conn.close()
#     return data


def get_symbols_from_database(sql_conn):
    sqlstr = "SELECT * FROM GetSymbolsInDatabase ORDER BY SecuritySymbol ASC"
    data = pd.read_sql_query(sql=sqlstr, con=sql_conn, index_col='SecuritySymbol')
    answer = OrderedDict()
    for i in data.index:
        answer[i] = data.loc[i, 'DataSourceName']
    return answer


def parse_creds(cred_file, database_name):
    with open(cred_file, "r") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    # database creds
    db_info = {}
    db_info['user'] = data['databases'][database_name]['user']
    db_info['password'] = data['databases'][database_name]['password']
    db_info['host'] = data['databases'][database_name]['host']

    # datasource creds
    data_source_info = {}
    for datasource in data['datasources']:
        data_source_info[datasource] = data['datasources'][datasource]

    return db_info, data_source_info


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-intraday", "--intraday", action="store_true", default=False)
    parser.add_argument("-daily", "--daily", action="store_true", default=False)
    args = parser.parse_args()

    logger = init_logger().info

    for dbname, truefalse in args._get_kwargs():
        if truefalse:
            database_name = "PRICES_{}".format(dbname.upper())

            logger("USING DATABASE: {}".format(database_name))

            db_info, data_source_info = parse_creds(cred_file, database_name=database_name)

            sql_conn, sql_cursor = mysql_connect(
                host=db_info['host'],
                user=db_info['user'],
                password=db_info['password'],
                database=database_name)

            tickers = get_etf_tickers()

            database_update(
                logger=logger,
                sql_conn=sql_conn,
                sql_cursor=sql_cursor,
                data_source_info=data_source_info,
                wait_seconds=5,
                tickers=tickers)
            sql_conn.close()
