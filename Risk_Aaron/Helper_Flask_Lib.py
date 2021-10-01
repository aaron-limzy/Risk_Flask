
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db

from flask_table import create_table, Col
from flask import url_for, session, current_app, request
import decimal
from Aaron_Lib import *
import pandas as pd
from unsync import unsync

import pytz

TIME_UPDATE_SLOW_MIN = 10
LP_MARGIN_ALERT_LEVEL = 20            # How much away from MC do we start making noise.


if get_machine_ip_address() == '192.168.64.73': #Only On Server computer
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com", "Risk@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com", "risk@blackwellglobal.com", "cs@bgifx.com"]
    print("On Server 64.73")
else:
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com"]
    print("On Aaron's Computer")

EMAIL_AARON =  ["aaron.lim@blackwellglobal.com"]     # For test Groups.
EMAIL_LIST_RISKTW = ["aaron.lim@blackwellglobal.com", "fei.shao@blackwellglobal.com", "nicole.cheng@blackwellglobal.com", "joyce.liou@blackwellglobal.com"]



@async_fun
def async_sql_insert(app, header="", values = [" "], footer = "", sql_max_insert=500):

    #print("Using async_sql_insert")

    with app.app_context():  # Using current_app._get_current_object()
        for i in range(math.ceil(len(values) / sql_max_insert)):
            # To construct the sql statement. header + values + footer.
            sql_trades_insert = header + " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]) + footer
            sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
            #print(sql_trades_insert)
            sql_trades_insert = text(sql_trades_insert)  # To make it to SQL friendly text.
            raw_insert_result = db.engine.execute(sql_trades_insert)
    return


@async_fun
def async_sql_insert_raw(app, sql_insert):

    #print("Using async_sql_insert")

    with app.app_context():  # Using current_app._get_current_object()
        # To make it to SQL friendly text.
        sql_trades_insert = text(sql_insert.replace("\n", " ").replace("\t", " "))
        raw_insert_result = db.engine.execute(sql_trades_insert)
    return

# Async update the runtime table for update.
@async_fun
def async_update_Runtime(app, Tool):

    #print("Running Async Tool time update: {}".format(Tool))

    # sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
    #              " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
    #                  Tool=Tool)
    # raw_insert_result = app.app_context().db.engine.execute(sql_insert)


    #start = time.perf_counter()
    with app.app_context(): # Using current_app._get_current_object()
        # Want to update the runtime table to ensure that tool is running.
        sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
                     " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
                         Tool=Tool)
        raw_insert_result = db.engine.execute(sql_insert)


        # print("Updating Runtime for Tool: {}".format(Tool))
    #total_time = time.perf_counter() - start
    #print('Total Time taken for non-sync SQL insert: {}'.format(total_time))



# Async Call to send email.
@async_fun
def async_send_email(To_recipients, cc_recipients, Subject, HTML_Text, Attachment_Name):
    Send_Email(To_recipients, cc_recipients, Subject, HTML_Text, Attachment_Name)


# Async Call to send telegram message.
@async_fun
def async_Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs, Parse_mode=""):

    if Parse_mode == "":
        Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs)
    else:
        Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs, Parse_mode = Parse_mode)

# To get the volume snap shot
# If there is no symbol, we will get the symbol.
# If there is, we will get the entity.
@unsync
def Get_Vol_snapshot(app, symbol="", book="", day_backwards_count=5, entities = []):

    #print("Get_Vol_snapshot symbol: '{}'".format(symbol))

    # Want to plot the SNOP SHOT Graph of open volume
    # This is the table that is Cleated every hour
    # We put in a failsaf of getting it 1 day only.
    # If there's no symbol, we want to select the symbol as well.
    if symbol != "":    # If we want to filter it by symbols too.
        symbol_condition = " AND SYMBOL like '%{symbol}%' ".format(symbol=symbol)
        select_column = " COUNTRY, NET_FLOATING_VOLUME, FLOATING_VOLUME, FLOATING_REVENUE, DATETIME  "
    else:
        symbol_condition = ""
        select_column = " SYMBOL, NET_FLOATING_VOLUME, FLOATING_VOLUME, FLOATING_REVENUE, DATETIME  "

    book_condition =  '  AND COUNTRY IN (SELECT COUNTRY from live5.group_table where BOOK = "{book}") '.format(book=book)

    if len(entities) > 0: # there's actually an entity here.
        country_condition = " AND COUNTRY IN ({})".format(" , ".join(["'{}'".format(c) for c in entities]))
        book_condition = " "    # No need for book condition.
    else:
        country_condition = " AND COUNTRY not like 'HK' "


    SQL_Query_Volume_Recent = """SELECT {select_column} 
        FROM aaron.bgi_float_history_save 
        WHERE DATETIME >= NOW() - INTERVAL 1 DAY
        {symbol_condition}
       {book_condition}
        {country_condition} """.format(select_column=select_column, symbol=symbol, book=book,
                                       country_condition=country_condition,
                                       symbol_condition=symbol_condition, book_condition=book_condition)

    #print(SQL_Query_Volume_Recent)

    SQL_current_Volume_Query = SQL_Query_Volume_Recent.replace("\n", " ").replace("\t", " ")

    # Use the unsync version to query SQL
    results_current = unsync_query_SQL_return_record(SQL_current_Volume_Query, app)
    df_data_vol_current = pd.DataFrame(results_current)

    day_backwards = "{}".format(get_working_day_date(datetime.date.today(), -1 * day_backwards_count))

    # Get those that are more grunular.
    # These are those that has just been saved, and have yet to be aggregated.

    SQL_Query_Volume_Days = """SELECT {select_column} 
        FROM aaron.bgi_float_history_past 
        WHERE datetime >= '{day_backwards}'
        {symbol_condition}       
        AND COUNTRY IN (SELECT COUNTRY from live5.group_table where BOOK = "{book}") 
        {country_condition} """.format(select_column=select_column, symbol=symbol, book=book,
                                       country_condition=country_condition,
                                       symbol_condition=symbol_condition, day_backwards=day_backwards)


    #SQL_Volume_Query = " UNION ".join([SQL_Query_Volume_Recent, SQL_Query_Volume_Days])
    #
    SQL_Query_Volume_Days = SQL_Query_Volume_Days.replace("\n", " ").replace("\t", " ")

    # Use the unsync version to query SQL
    results = unsync_query_SQL_return_record(SQL_Query_Volume_Days, app)
    df_data_vol_past = pd.DataFrame(results)


    # Want to clean up rouge datetime that only appears once.
    if "DATETIME" in df_data_vol_past:
        df_data_vol_past = df_data_vol_past[df_data_vol_past["DATETIME"].isin(clear_df_datetime(df_data_vol_past))]


    # concat the past, and the currently saved data together.
    df_data_vol = pd.concat([df_data_vol_current, df_data_vol_past])

    # If it's empty. Return empty DataFrame
    if len(df_data_vol) == 0:
        return pd.DataFrame([])

    # Drop Duplicate as there's a chance that the same data are in Past, and current data
    df_data_vol.drop_duplicates(inplace=True)

    # print("Get_Vol_snapshot: ")
    # print(df_data_vol)

    # We want to show a Total Volume, if it's just 1 symbol.
    # It would make sense to add them up.
    if "symbol" != "":
        df_data_vol_total = df_data_vol.groupby("DATETIME").sum().reset_index()
        df_data_vol_total["COUNTRY"] = "TOTAL"
        #print(df_data_vol_total)
        df_data_vol  = pd.concat([df_data_vol, df_data_vol_total])


    return df_data_vol


@unsync
def Symbol_history_Daily(app, symbol="", book="B", day_backwards_count = 15, entities=[]):

    day_backwards = "{}".format(get_working_day_date(datetime.date.today(), -1 * day_backwards_count))

    book_condition =  "  AND COUNTRY IN (Select COUNTRY FROM live5.group_table WHERE Group_table.BOOK = '{book}') ".format(book=book)


    if len(entities) > 0: # there's actually an entity here.
        country_condition = " AND COUNTRY IN ({})".format(" , ".join(["'{}'".format(c) for c in entities]))
        book_condition = " "
    else:
        country_condition = " AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK')   "

    if symbol != "":    # If we want to filter it by symbols too.
        symbol_condition = " AND SYMBOL like '%{symbol}%' ".format(symbol=symbol)
    else:
        symbol_condition = " "


    sql_statement = """SELECT DATE, COUNTRY, SYMBOL, VOLUME, REVENUE 
    FROM `bgi_dailypnl_by_country_group`
    WHERE  DATE >= '{day_backwards}' {country_condition} 
    {book_condition}
    {symbol_condition} 
    ORDER BY DATE""".format(day_backwards=day_backwards, symbol=symbol,
                            country_condition=country_condition,
                            book_condition=book_condition, symbol_condition=symbol_condition)
    #print(sql_statement)
    sql_query = sql_statement.replace("\n", " ").replace("\t", " ")

    return_val = unsync_query_SQL_return_record(sql_query, app)
    #print(return_val)
    return return_val

# Get trades by open tim and SYMBOLs
# Want to later use to plot volume vs open_time
# Can choose A Book or B Book.
@unsync
def symbol_opentime_trades(app, symbol="", book="B", start_date="", entities=""):

    #symbol="XAUUSD"
    #print("Querying for symbol_opentime_trades. Symbol:{}, book:{}, start_date:{}".format(symbol,book,start_date))

    # Symbol condition, if query is for specific symbols
    if len(symbol) > 0:
        symbol_condition = " AND SYMBOL Like '%{}%' ".format(symbol)
    else:
        symbol_condition = " "

    country_condition = "  "

    # Only need to set if there is no entity, or when book = b
    if book.lower() == "b" or  len(entities) <= 0:
        book_condition = " AND group_table.BOOK = '{}'".format(book)
        country_condition = " AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK')   "
    else:
        book_condition = " "  # No need for book condition.


    if len(entities) > 0: # there's actually an entity here.
        country_condition = " AND COUNTRY IN ({})".format(" , ".join(["'{}'".format(c) for c in entities]))
        book_condition = " "  # No need for book condition.


    # A book, with no entity given.
    if book.lower() == "a" and len(entities) <= 0:
        #print("Will give unique country conditions.")
        book_condition = " " # No need for this. We will write our own.

        country_condition_raw = """ AND ((mt4_trades.`GROUP` IN(SELECT `GROUP` FROM live{live}.a_group))
                                 OR
                                 (LOGIN IN( SELECT LOGIN FROM live{live}.a_login))
                                 ) """


        country_condition_Live1 = country_condition_raw.format(live=1)
        country_condition_Live2 = """ AND	(
            (mt4_trades.`GROUP` IN(SELECT `GROUP` FROM live2.a_group))
            OR (LOGIN IN(SELECT LOGIN FROM live2.a_login))
            OR LOGIN = '9583'
            OR LOGIN = '9615'
            OR LOGIN = '9618'
            OR(mt4_trades.`GROUP` LIKE 'A_ATG%' AND VOLUME > 1501)
            ) """
        country_condition_Live3 = country_condition_raw.format(live=3)
        country_condition_Live5 = country_condition_raw.format(live=5)
    else:
        # If not a book, or if there is entity in query, we will use the standard country_condition.
        #print("Will give standard country conditions.")
        country_condition_Live1 = country_condition
        country_condition_Live2 = country_condition
        country_condition_Live3 = country_condition
        country_condition_Live5 = country_condition


    # Live 1,2,3
    OPEN_TIME_LIMIT = "{} 22:00:00".format(start_date)

    # Live 5
    # Also did  DATE_SUB(OPEN_TIME, INTERVAL 1 HOUR)  For the open time.
    OPEN_TIME_LIMIT_L5 = "{} 23:00:00".format(start_date)



    sql_statement = """ SELECT 	LIVE,
        COUNTRY, CMD, SUM(LOTS) as 'LOTS',
        OPEN_PRICE, OPEN_TIME, CLOSE_PRICE,
        CLOSE_TIME, SUM(SWAPS) as 'SWAPS',
        SUM(PROFIT) as 'PROFIT', `GROUP` 
        FROM ((SELECT
                'live1' AS LIVE, group_table.COUNTRY, CMD,
                VOLUME * 0.01 AS LOTS, OPEN_PRICE,  OPEN_TIME, CLOSE_PRICE, CLOSE_TIME,
                SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM
                live1.mt4_trades, live5.group_table
            WHERE
                mt4_trades.`GROUP` = group_table.`GROUP`
            AND mt4_trades.OPEN_TIME >= '{OPEN_TIME_LIMIT}'
            AND LENGTH(mt4_trades.SYMBOL)> 0
            AND mt4_trades.CMD < 2
            AND group_table.LIVE = 'live1'
            AND LENGTH(mt4_trades.LOGIN)> 4 {symbol_condition} {country_condition_Live1} {book_condition}
        )
            UNION 
        (
            SELECT
                'live2' AS LIVE, group_table.COUNTRY, CMD,
                VOLUME * 0.01 AS LOTS, OPEN_PRICE, OPEN_TIME, CLOSE_PRICE, CLOSE_TIME,
                SWAPS, PROFIT, mt4_trades.`GROUP`

            FROM
                live2.mt4_trades, live5.group_table
            WHERE
                mt4_trades.`GROUP` = group_table.`GROUP`
            AND mt4_trades.OPEN_TIME >= '{OPEN_TIME_LIMIT}'
            AND LENGTH(mt4_trades.SYMBOL)> 0
            AND mt4_trades.CMD < 2
            AND group_table.LIVE = 'live2'
            AND LENGTH(mt4_trades.LOGIN)> 4 {symbol_condition} {country_condition_Live2} {book_condition}
        )
            UNION 
        (
            SELECT
                'live3' AS LIVE, group_table.COUNTRY, CMD, VOLUME * 0.01 AS LOTS,
                OPEN_PRICE, OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM
                live3.mt4_trades, live5.group_table
            WHERE
                mt4_trades.`GROUP` = group_table.`GROUP`
            AND mt4_trades.OPEN_TIME >= '{OPEN_TIME_LIMIT}'
            AND LENGTH(mt4_trades.SYMBOL)> 0
            AND mt4_trades.CMD < 2
            AND group_table.LIVE = 'live3'
            AND LENGTH(mt4_trades.LOGIN)> 4
            AND mt4_trades.LOGIN NOT IN(SELECT LOGIN FROM live3.cambodia_exclude){symbol_condition} {country_condition_Live3} {book_condition}
        )
            UNION
        (
            SELECT
                'live5' AS LIVE, group_table.COUNTRY, CMD, VOLUME * 0.01 AS LOTS, OPEN_PRICE,
                DATE_SUB(OPEN_TIME, INTERVAL 1 HOUR) , CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM
                live5.mt4_trades,
                live5.group_table
            WHERE
                mt4_trades.`GROUP` = group_table.`GROUP`
            AND mt4_trades.OPEN_TIME >= '{OPEN_TIME_LIMIT_L5}'
            AND LENGTH(mt4_trades.SYMBOL)> 0
            AND mt4_trades.CMD < 2
            AND group_table.LIVE = 'live5'
            AND LENGTH(mt4_trades.LOGIN)> 4 {symbol_condition} {country_condition_Live5} {book_condition}
        ))AS A
        GROUP BY COUNTRY, LEFT(OPEN_TIME, 16), `GROUP`""".format(OPEN_TIME_LIMIT=OPEN_TIME_LIMIT,
                                                                 OPEN_TIME_LIMIT_L5=OPEN_TIME_LIMIT_L5,
                                                                 symbol_condition=symbol_condition,
                                                                 country_condition_Live1=country_condition_Live1,
                                                                 country_condition_Live2=country_condition_Live2,
                                                                 country_condition_Live3=country_condition_Live3,
                                                                 country_condition_Live5=country_condition_Live5,
                                                                 book_condition=book_condition,)

    sql_query = sql_statement.replace("\n", " ").replace("\t", " ")
    #print(sql_query)

    return unsync_query_SQL_return_record(sql_query, app)





@unsync
# # Get all open trades of a particular symbol.
# # Get it converted as well.
# # Can choose A Book or B Book.
def symbol_all_open_trades(app, ServerTimeDiff_Query, symbol="", book="B", entities=[]):
    # symbol="XAUUSD"

    # Symbol condition, if query is for specific symbols
    if len(symbol) > 0:  # If we want to filter by Symbol.
        symbol_condition = " AND mt4_trades.SYMBOL Like '%{}%' ".format(symbol)
    else:
        symbol_condition = " "

    # Default parameters.
    country_condition = "  "
    book_condition = " AND group_table.BOOK = '{}'".format(book)

    # IF B book, or if there are no entity.
    # We will need to set the country and book condition
    if book.lower() == "b" or len(entities) == 0 or entities[0] == "":
        country_condition = " AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK') "
        book_condition = " AND group_table.BOOK = '{}'".format(book)

    if len(entities) > 0:  # there's actually an entity here.
        country_condition = " AND COUNTRY IN ({})".format(" , ".join(["'{}'".format(c) for c in entities]))
        book_condition = " "  # No need for book condition.

    # A book, with no entity given.
    if book.lower() == "a" and len(entities) <= 0:
        #print("Will give unique country conditions.")
        book_condition = " "  # No need for this. We will write our own.

        country_condition_raw = """ AND ((mt4_trades.`GROUP` IN(SELECT `GROUP` FROM live{live}.a_group))
                                 OR
                                 (LOGIN IN( SELECT LOGIN FROM live{live}.a_login))
                                 ) """

        country_condition_Live1 = country_condition_raw.format(live=1)
        country_condition_Live2 = """ AND	(
            (mt4_trades.`GROUP` IN(SELECT `GROUP` FROM live2.a_group))
            OR (LOGIN IN(SELECT LOGIN FROM live2.a_login))
            OR LOGIN = '9583'
            OR LOGIN = '9615'
            OR LOGIN = '9618'
            OR(mt4_trades.`GROUP` LIKE 'A_ATG%' AND VOLUME > 1501)
            ) """
        country_condition_Live3 = country_condition_raw.format(live=3)
        country_condition_Live5 = country_condition_raw.format(live=5)
    else:
        # If not a book, or if there is entity in query, we will use the standard country_condition.
        #print("Will give standard country conditions.")
        country_condition_Live1 = country_condition
        country_condition_Live2 = country_condition
        country_condition_Live3 = country_condition
        country_condition_Live5 = country_condition



    sql_statement = """(SELECT 'live1' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A1.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE
        FROM (

        SELECT 'live1' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,
           ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
        FROM live1.mt4_trades, live5.group_table
        WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00'  
                OR mt4_trades.CLOSE_TIME >= (CASE WHEN 
                    HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                    ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live1' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition} {country_condition_Live1} {book_condition}
            ) as A1 LEFT JOIN  live1.symbol_rebate as B1 ON A1.SYMBOL = B1.SYMBOL)
    UNION 
        (SELECT 'live2' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A2.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE
        FROM 
        (SELECT 'live2' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live2.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' 
                or mt4_trades.CLOSE_TIME >= (CASE WHEN 
                    HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                    ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live2' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition} {country_condition_Live2} {book_condition}) as A2 LEFT JOIN  live2.symbol_rebate as B2 ON A2.SYMBOL = B2.SYMBOL)
    UNION (
    SELECT 'live3' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A3.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE
    FROM 
        (SELECT 'live3' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live3.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' 
                or mt4_trades.CLOSE_TIME >= (CASE WHEN 
                    HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                    ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live3' 
            AND LENGTH(mt4_trades.LOGIN)>4 
            AND mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM live3.cambodia_exclude) 
            {symbol_condition} {country_condition_Live3} {book_condition}) as A3 LEFT JOIN  live3.symbol_rebate as B3 ON A3.SYMBOL = B3.SYMBOL)
    UNION
        (SELECT 'live5' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A5.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE
     FROM 
        (SELECT 'live5' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live5.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or 
            mt4_trades.CLOSE_TIME >= DATE_ADD(
                    (CASE WHEN 
                        HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                        ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END),
                    INTERVAL 1 HOUR)
            )
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live5' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition} {country_condition_Live5} {book_condition}) 
            as A5 LEFT JOIN  live5.symbol_rebate as B5 ON A5.SYMBOL = B5.SYMBOL)
            """.format(symbol_condition=symbol_condition,
                       ServerTimeDiff_Query=ServerTimeDiff_Query,
                       book_condition=book_condition,
                       country_condition_Live1=country_condition_Live1,
                       country_condition_Live2=country_condition_Live2,
                       country_condition_Live3=country_condition_Live3,
                       country_condition_Live5=country_condition_Live5)

    # print(sql_statement)
    sql_query = sql_statement.replace("\n", " ").replace("\t", " ")
    #raw_result = db.engine.execute(sql_query)  # Insert select..
    #result_data = raw_result.fetchall()  # Return Result
    #result_col = raw_result.keys()  # Column names

    return unsync_query_SQL_return_record(sql_query, app)




@unsync
# # Get all open trades of a particular symbol.
# # Get it converted as well.
# # Can choose A Book or B Book.
def group_symbol_all_open_trades(app, ServerTimeDiff_Query, group):
    # symbol="XAUUSD"


    group_condition = " AND mt4_trades.`group` like '{}'".format(group)


    sql_statement = """(SELECT 'live1' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A1.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE, BOOK
        FROM (

        SELECT 'live1' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`, BOOK,
           ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
        FROM live1.mt4_trades, live5.group_table
        WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00'  
                OR mt4_trades.CLOSE_TIME >= (CASE WHEN 
                    HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                    ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live1' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {group_condition} ) as A1 LEFT JOIN  live1.symbol_rebate as B1 ON A1.SYMBOL = B1.SYMBOL)
    UNION 
        (SELECT 'live2' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A2.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE, BOOK
        FROM 
        (SELECT 'live2' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,BOOK,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live2.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' 
                or mt4_trades.CLOSE_TIME >= (CASE WHEN 
                    HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                    ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live2' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {group_condition}) as A2 LEFT JOIN  live2.symbol_rebate as B2 ON A2.SYMBOL = B2.SYMBOL)
    UNION (
    SELECT 'live3' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A3.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE, BOOK
    FROM 
        (SELECT 'live3' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,BOOK,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live3.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' 
                or mt4_trades.CLOSE_TIME >= (CASE WHEN 
                    HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                    ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live3' 
            AND LENGTH(mt4_trades.LOGIN)>4 
            AND mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM live3.cambodia_exclude) 
            {group_condition}) as A3 LEFT JOIN  live3.symbol_rebate as B3 ON A3.SYMBOL = B3.SYMBOL)
    UNION
        (SELECT 'live5' AS LIVE,		
		COUNTRY,		LOGIN,		TICKET,		A5.SYMBOL,		CMD,		LOTS,
		OPEN_PRICE,		OPEN_TIME,		CLOSE_PRICE,		CLOSE_TIME,		SWAPS,		PROFIT,		`GROUP`, COALESCE(REBATE,0) * LOTS as `REBATE`, CONVERTED_REVENUE, BOOK
     FROM 
        (SELECT 'live5' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,BOOK,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live5.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or 
            mt4_trades.CLOSE_TIME >= DATE_ADD(
                    (CASE WHEN 
                        HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') 
                        ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END),
                    INTERVAL 1 HOUR)
            )
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live5' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {group_condition}) 
            as A5 LEFT JOIN  live5.symbol_rebate as B5 ON A5.SYMBOL = B5.SYMBOL)
            """.format(group_condition=group_condition,
                       ServerTimeDiff_Query=ServerTimeDiff_Query,
                       )

    #print(sql_statement)
    sql_query = sql_statement.replace("\n", " ").replace("\t", " ")
    #raw_result = db.engine.execute(sql_query)  # Insert select..
    #result_data = raw_result.fetchall()  # Return Result
    #result_col = raw_result.keys()  # Column names

    return unsync_query_SQL_return_record(sql_query, app)




# Query SQL and return the Zip of the results to get a record.
def unsync_query_SQL_return_record(SQL_Query, app):

    results = [{}]
    with app.app_context():  # Need to use original app as this is in the thread

        raw_result = db.engine.execute(text(SQL_Query))
        result_data = raw_result.fetchall()
        result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d] for d in
                               result_data]  # correct The decimal.Decimal class to float.
        result_col = raw_result.keys()
        results = [dict(zip(result_col, d)) for d in result_data_decimal]
    return results


# This function to be the unsync function to call the sync SQL call.
@unsync
def unsync_query_SQL_return_record_fun(SQL_Query, app):
    return unsync_query_SQL_return_record(SQL_Query, app)


# If we can run C progs on unsync to save time.
@unsync
def unsync_Run_C_Prog(Path, cwd=None):
    #print(os.getcwd())
    return Run_C_Prog(Path, cwd=cwd)





def create_table_fun(table_data, additional_class=[]):

    T = create_table()
    table_class = additional_class + ["table", "table-striped", "table-bordered", "table-hover", "table-sm"]
    table = T(table_data, classes=table_class)
    if (len(table_data) > 0) and isinstance(table_data[0], dict):
        for c in table_data[0]:
            if c != "\n":
                table.add_column(c, Col(c, th_html_attrs={"style": "background-color:# afcdff; word-wrap:break-word"}))
    return table

# Simple way of returning time string.
def time_now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Query SQL and return the Zip of the results to get a record.
def query_SQL_return_record(SQL_Query):
    raw_result = db.engine.execute(SQL_Query)
    result_data = raw_result.fetchall()
    result_col = raw_result.keys()
    collate = [dict(zip(result_col, a)) for a in result_data]
    return collate


# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_db_engine(sql_query):
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results


# Insert into 64.73. (NOT async)
# Will have to wait for it to be done.
def Insert_into_sql(sql_Insert_query):
    sql_insert = sql_Insert_query.replace("\t", "").replace("\n", "")
    sql_insert = text(sql_insert)  # To make it to SQL friendly text.
    raw_insert_result = db.engine.execute(sql_insert)
    return


# Query 10.25 Data base.
# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_Symbol_Markup_db_engine(sql_query):

    raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'mt5_futures'))
    #raw_result = db5.engine.execute(text(sql_query))

    #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
    #return raw_result

    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results





# Gets in a Pandas dataframe.
# Want to calculate what is the net position of the login
def Calculate_Net_position(df_data):

    # Need to check that all the needed column is in there before we can start calculating.
    if not all(i in df_data for i in ['CLOSE_TIME', 'CMD', 'LOTS', 'PROFIT', 'SWAPS', 'SYMBOL']):
        return (pd.DataFrame([{"Results": "No Open Trades"}]))  # Return empty dataframe

    df_data = df_data[df_data["CMD"] < 2]   # Only want Buy and Sell
    df_data = df_data[df_data["CLOSE_TIME"] == pd.Timestamp('1970-01-01 00:00:00')] # Only open trades.

    if len(df_data) < 1:
        return (pd.DataFrame([{"Results": "No Open Trades"}])) # Return empty dataframe



    df_data["NET_LOTS"] = df_data.apply(lambda x: x["LOTS"] if x["CMD"] == 0 else -1*x["LOTS"], axis=1)   # -ve for sell
    #df_data["LOTS"] = round(df_data['LOTS'].sum(), 2)


    df_data = df_data[['SYMBOL', 'LOTS', 'NET_LOTS', 'PROFIT', 'SWAPS' ]]   # Only need these few.
    ret_val = df_data.groupby(['SYMBOL']).sum()     # Want to group by Symbol, and sum
    ret_val.reset_index(level=0, inplace=True)      # Want to reset the index so that "SYMBOL" becomes the column name
    ret_val["NET_LOTS"] = ret_val["NET_LOTS"].apply(round, 2)
    ret_val["PROFIT"] = ret_val["PROFIT"].apply(profit_red_green) # Print in 2 D.P,with color (HTML)
    ret_val["SWAPS"] = ret_val["SWAPS"].apply(profit_red_green) # Print in 2 D.P,with color (HTML)


    ret_val["LOTS"] = ret_val["LOTS"].apply(lambda x: "{:.2f}".format(x))  # Print in 2 D.P.

    return ret_val


# Want toquery SQL for derived data such as Total Deposit, Withdrawal, Profit, Floating Profit, Total Lots
# And how many lots winning, How many lots loosing.. etc etc..
def Sum_total_account_details(Live, Login):

    sql_statement="""SELECT 
        ROUND(COALESCE(SUM(CASE WHEN PROFIT > 0 AND CMD = 6 THEN PROFIT  END),0),2) as "DEPOSIT", 
        ROUND(COALESCE(SUM(CASE WHEN PROFIT < 0 AND CMD = 6 THEN PROFIT  END),0),2) as "WITHDRAWAL", 
        ROUND(COALESCE(SUM(CASE WHEN CMD <2 and CLOSE_TIME != "1970-01-01 00:00:00" THEN PROFIT + SWAPS  END),0),2) as "CLIENT PROFIT",
        ROUND(COALESCE(SUM(CASE WHEN CMD <2 and CLOSE_TIME = "1970-01-01 00:00:00" THEN PROFIT + SWAPS END),0),2) as "FLOATING PROFIT", 
        ROUND(COALESCE(SUM(CASE WHEN CMD < 2 AND CLOSE_TIME != "1970-01-01 00:00:00" THEN VOLUME * 0.01 END),0),2) as "LOTS",
                COALESCE(SUM(CASE  WHEN CMD <2 and CLOSE_TIME != "1970-01-01 00:00:00" AND (PROFIT+SWAPS) > 0 THEN 1 END),0) as "NUM PROFIT TRADES",
				COALESCE(SUM(CASE  WHEN CMD <2 and CLOSE_TIME != "1970-01-01 00:00:00" AND (PROFIT+SWAPS) < 0 THEN 1 END),0) as "NUM LOSING TRADES"
    FROM live{Live}.mt4_trades 
    where `Login`='{Login}'""".format(Live=Live, Login=Login)
    sql_statement = sql_statement.replace("\n", "").replace("\t", "")
    account_details_list = Query_SQL_db_engine(sql_statement)
    account_details=account_details_list[0] # since we only expect 1 reply from SQL



    account_details["% PROFIT"] = "No Deposit" if account_details["DEPOSIT"] == 0 else  "{:.2f}%".format(100 * round(account_details["CLIENT PROFIT"] / account_details["DEPOSIT"],4))       # The % of profit from total deposit
    account_details["PER LOT AVERAGE"] = "No Lots" if account_details["LOTS"] == 0 else  \
           round(account_details["CLIENT PROFIT"] / account_details["LOTS"],2)    # The Profit per lot.
    account_details["% WINNING TRADES"] = "No Trades Found" if  (account_details["NUM LOSING TRADES"] + account_details["NUM PROFIT TRADES"]) == 0 else \
        "{:.2f}%".format(100 * round(account_details["NUM PROFIT TRADES"] / (account_details["NUM LOSING TRADES"] + account_details["NUM PROFIT TRADES"]),4))



    return [account_details]

# Function that takes in df for trades
# Calculate Average trade time per symbol
def Average_trade_time_per_symbol(df_data):
    # Need to check that all the needed column is in there before we can start calculating.
    if not all(i in df_data for i in ['CLOSE_TIME', 'OPEN_TIME', 'LOTS', 'PROFIT', 'SWAPS', 'SYMBOL']):
        return (pd.DataFrame())  # Return empty dataframe


    # Get the Duration in seconds
    df_data["DURATION"] = df_data.apply(lambda x: (x["CLOSE_TIME"] - x["OPEN_TIME"]).seconds, axis=1)
    symbol_duration_avg = df_data[["SYMBOL", "DURATION"]].groupby('SYMBOL').mean()    # Want to group by, for seconds
    symbol_duration_avg.reset_index(level=0, inplace=True)  # Want to reset the index so that "SYMBOL" becomes the column name
    symbol_duration_avg.sort_values(by=["DURATION"], ascending=True, inplace=True)
    symbol_duration_avg["DURATION"] = symbol_duration_avg["DURATION"].round(2)  # Want to set to 2 DP only.
    return symbol_duration_avg

# Gives the duration in seconds
def trade_duration_bin(duration):
    # Want to do 1 min, 2 mins, 3 mins, 5 mins, 10 mins and an hour
    bin_in_seconds = {60: "<span style='background-color:#fc6547'><= 1 min</span>",
                      120: "<span style='background-color:#fc9547'>1-2 mins</span>",
                      180 : "<span style='background-color:#fcba47'>2-3 mins</span>",
                      300: "<span style='background-color:#fcd847'>3-5 mins</span>",
                      600:"<span style='background-color:#fcf947'>5 mins - 10 Mins</span>",
                      3600 : "10 mins - 1 hour",
                      7200 : "1 hour - 2 hours",
                      14400: "2 hours - 4 hours",
                      86400: "4 hours - 1 day",
                      259200: "1 day - 3 days",
                      604800: "3 days - 1 Week",
                      2592000: "1 Week - 1 Month"
                      }

    for d, s in bin_in_seconds.items():
        if duration <= d:   # If less then or equals to
            return s
    return "> 1 Month"

# To get the URL for the Live/Login to run client analysis
def live_login_analysis_url(Live, Login):

    url = url_for("analysis.Client_trades_Analysis", Live=int(Live),  Login=int(Login), _external=True)
    return '<a href="{url}">{Login}</a>'.format(url=url,  Login=Login)


# To get the URL for the Live/Login to run client analysis
# For telegram. Will replace localhost with the external IP
#TODO: Need to put external IP into a seperate File for easy change.
def live_login_analysis_url_External(Live, Login):

    url = url_for("analysis.Client_trades_Analysis", Live=int(Live),  Login=int(Login), _external=True)
    return '<a href="{url}">{Login}</a>'.format(url=url,  Login=Login).replace("localhost", "202.88.105.3")


# To get the URL for the Symbol Trades A/B Book
def Symbol_Trades_url(symbol, book):

    url = url_for("analysis.symbol_float_trades", symbol=symbol, book=book, _external=True)
    return '<a href="{url}" target="_blank">{symbol}</a>'.format(url=url,  symbol=symbol)

# To get client Group URL
def client_group_url(group):

    url = url_for("analysis.group_float_trades", group=group)
    return '<a href="{url}" target="_blank">{group}</a>'.format(url=url,  group=group)


# To get the URL for the Symbol Trades A/B Book
# If we want to hyperlink, but add different text, fill in optional text parameter.
# Only when text is empty will the word be blue. Else, it will be black.
def Country_Trades_url(country, text=""):

    # Want to append text, if there is. Else, default to show country.
    text = text if text != "" else country
    # Want to add in that the style is black if there are no added text.
    # ie: Text is black when text != country
    added_style = 'style = "color:black" ' if text != country else " "
    url = url_for("analysis.Country_float_trades", country=country)

    return '<a href="{url}" {added_style} target="_blank">{text}</a>'.format(url=url, added_style=added_style, text=text)

# Attempt to get the root trading Symbol.
# For FX PM and CFDs
def split_root_symbol(sym):

    if sym.find(".") == 0: # CFD
        cfd_list = [".A50", ".AUS200", ".DE40", ".ES35", ".F40", ".HK50", ".JP225",
            ".STOXX50", ".UK100", ".UKOil",  ".US100", ".US30", ".US500", ".USOil"]
        for c in cfd_list:
            if sym.find(c) == 0: # Found that CFD
                return c
    else:
        return sym[:6] if len(sym) >= 6 else sym

    return sym # Cannot find anything. Default to returning the actual symbol


# color the text green if positive, red if positive.
def profit_red_green(x):
    color="black" # By default the color is black
    if float(x) > 0:
        color = "green"
    elif float(x) < 0:
        color = "red"

    # This is for -0.00 correction
    if x==0:
        x = 0
    return "<span style='color:{color}'>{x:,.2f}</span>".format(color=color,x=float(x))



# color the rebate.
# If PnL was -ve, but is +ve after rebate.
# Will want to flag it out.
# multiplier is if we need to flip.
def color_rebate(rebate, pnl, multiplier = 1):
    style = "" # By default
    #style = " style='background-color:#FF8065' "
    if float(pnl) <= 0 and (float(rebate) + float(pnl)) >= 0:
         style = " style='background-color:#FF8065' "
    return "<span {style}>{x:,.2f}</span>".format(style=style, x=round(float(rebate) * multiplier,2))



def color_negative_red(value):
      # """
      # Colors elements in a dateframe
      # green if positive and red if
      # negative. Does not color NaN
      # values.
      # """

    if value < 0:
        color = 'red'
    elif value > 0:
        color = 'green'
    else:
        color = 'black'
    #return 'color: %s' % color
    return  color


# Will alter the session details.
# does not return anything
def check_session_live1_timing():

    test = False
    # if test == True:
    #     if "live1_sgt_time_update" in session:
    #         print("session['live1_sgt_time_update'] = {}".format(session["live1_sgt_time_update"]))
    #         print(' datetime.datetime.now() < session["live1_sgt_time_update"] : {}'.format(
    #             (datetime.datetime.now() + datetime.timedelta(hours=3)) < session["live1_sgt_time_update"]))
    #         print()
    #
    #
    #     if  "FLASK_UPDATE_TIMING" in session:
    #         print('current_app.config["FLASK_UPDATE_TIMING"] = {}'.format(current_app.config["FLASK_UPDATE_TIMING"]))
    #         print('session["FLASK_UPDATE_TIMING"] = {}'.format( session["FLASK_UPDATE_TIMING"]))
    #         print(session["FLASK_UPDATE_TIMING"]  == current_app.config["FLASK_UPDATE_TIMING"])


    return_val = False  # If session timing is outdated, or needs to be updated.
    # Might need to set the session life time. I think?
    # Saving some stuff in session so that we don't have to keep querying for it.

    # if "live1_sgt_time_update" in session:
    #     print(session["live1_sgt_time_update"].replace(tzinfo=None))
    #     print(type(session["live1_sgt_time_update"]))
    #
    #     print(session["live1_sgt_time_update"].astimezone( pytz.timezone('UTC')))

        #print(pytz.timezone('UTC').localize(session["live1_sgt_time_update"]))


    if "live1_sgt_time_diff" in session and  \
        "live1_sgt_time_update" in session and  datetime.datetime.now() < session["live1_sgt_time_update"].replace(tzinfo=None) and \
            'FLASK_UPDATE_TIMING' in session and  session["FLASK_UPDATE_TIMING"]  == current_app.config["FLASK_UPDATE_TIMING"]:
        return_val = True
        #print(session.keys())
        #print("From session: {}. Next update time: {}".format(session['live1_sgt_time_diff'], session['live1_sgt_time_update']))
    else:
        print(session)
        clear_session_cookies()    # Clear all cookies. And reload everything again.

        print("Refreshing cookies automatically in Flask")
        session['live1_sgt_time_diff'] = get_live1_time_difference()

        # Get the updated flask timing. This is when Flask re-runs on the server. To update any changes.
        session["FLASK_UPDATE_TIMING"] = current_app.config["FLASK_UPDATE_TIMING"]

        # Will get the timing that we need to update again.
        # Want to get either start of next working day in SGT, or in x period.
        if test == True:    # If we are testing if the cookies will be refreshed.
            time_refresh_next = datetime.datetime.now() + datetime.timedelta(minutes=2)
        else:
            time_refresh_next = datetime.datetime.now() + datetime.timedelta(hours=2, minutes=45)


        # need to add 10 mins, for roll overs and swap updates.
        server_nextday_time =  liveserver_Nextday_start_timing(
                    live1_server_difference=session['live1_sgt_time_diff'], hour_from_2300=0) + \
                                           datetime.timedelta(hours=session['live1_sgt_time_diff'], minutes=10)
        session['live1_sgt_time_update'] = min(time_refresh_next, server_nextday_time)
        # Post_To_Telegram(AARON_BOT, "Clearing cookies and retrieving new cookies for: {}".format(current_user.id),
        #                   TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)
        #print(session)

    return return_val


# Using Live 5 timing as guide.
# Since live 5 uses 0000 - 2400
# Will minus 1 day, and take 2300 to give live 1 timing
def liveserver_Nextday_start_timing(live1_server_difference=6, hour_from_2300 = 0, time = 0):

    if time == 0:   # Check if we had a start time
        now = datetime.datetime.now()
    else:
        now = time
    # Weekday: Minus how many days.
    # 6 = Sunday. We want to go ahead how many days, if it' that weekday (in key)
    next_day_start = {0: 1, 1: 1, 2: 1, 3: 1, 4: 3, 5: 2, 6: 1}

    # + 1 hour to force it into the next day.
    live5_server_timing = now - datetime.timedelta(hours=live1_server_difference) + datetime.timedelta(hours=1)
    return_time = get_working_day_date(start_date=live5_server_timing,
                        weekdays_count=  next_day_start[live5_server_timing.weekday()],
                                       weekdaylist=[0, 1, 2, 3, 4, 5, 6])
    return_time = return_time - datetime.timedelta(days=1)  # minus 1 days, and use 2300hrs
    return_time = return_time.replace(hour=23, minute=0, second=0, microsecond=0)
    return_time = return_time + datetime.timedelta(hours=hour_from_2300)

    #print("server_Time:{}, start_time: {}".format(live1_server_timing, return_time))
    return return_time


def clear_session_cookies():

    list_of_pop = []
    # We want to clear everything, other then the system generated ones.
    for u in list(session.keys()):
        if u not in  ['_fresh', '_id', 'csrf_token']:
            session.pop(u, None)
            list_of_pop.append(u)

    return list_of_pop


# Get live 1 time difference from server.
# SQL Table where aaron_misc_data` where item = 'live1_time_diff
def get_live1_time_difference():

    # MYSQL WEEKDAY FUNCTION
    #0 = Monday, 1 = Tuesday, 2 = Wednesday, 3 = Thursday, 4 = Friday, 5 = Saturday, 6 =Sunday

    server_time_diff_str = "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"
    sql_query = text(server_time_diff_str)

    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result

    return int(result_data[0][0])   # Return the integer value.



# Query SQL to return the previous day's PnL By Symbol
def get_mt4_symbol_daily_pnl():

    # Want to check what is the date that we should be retrieving.
    # Trying to reduce the over-heads as much as possible.
    live1_server_difference = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()

    live1_start_time = liveserver_Previousday_start_timing(live1_server_difference=live1_server_difference, hour_from_2300=5)
    date_of_pnl = "{}".format(live1_start_time.strftime("%Y-%m-%d"))

    sql_statement = """SELECT SYMBOL, SUM(VOLUME) AS VOLUME, SUM(REVENUE) AS REVENUE, DATE
            FROM aaron.`bgi_dailypnl_by_country_group`
            WHERE DATE = '{}' 
            AND COUNTRY in (SELECT DISTINCT(COUNTRY) from live5.group_table where BOOK = "B")
            AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK')
            GROUP BY SYMBOL""".format(date_of_pnl)

    # Want to get results for the above query, to get the Floating PnL
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result
    # print("result_data:")
    # print(result_data)
    # print("\n")
    result_col = raw_result.keys()  # The column names

    # If empty, we just want to return an empty data frame. So that the following merge will not cause any issues
    return_df = pd.DataFrame(result_data, columns=result_col) if len(result_data) > 0 else pd.DataFrame()
    return return_df

# Want to get the Live 1 START of the day timing
# Will do a comparison and get the time for start and end.
# Need to account for Live 5/MT5 timing.
# For today, if Live 1 hour < 23, will return 2 workind days ago 2300
def liveserver_Previousday_start_timing(live1_server_difference=6, hour_from_2300 = 0):

    # Weekday: Minus how many days.
    # 6 = Sunday, we want to take away 3 days, to Thursday starting.
    previous_day_start = {6:3, 0:4, 1:2, 2:2, 3:2, 4:2, 5:2}
    now = datetime.datetime.now()
    # + 1 hour to force it into the next day.
    live1_server_timing = now - datetime.timedelta(hours=live1_server_difference) + datetime.timedelta(hours=1)
    return_time = get_working_day_date(start_date=live1_server_timing, weekdays_count= -1 * previous_day_start[live1_server_timing.weekday()],
                                       weekdaylist=[0, 1, 2, 3,4,5,6])
    return_time = return_time.replace(hour=23, minute=0, second=0, microsecond=0)
    return_time = return_time + datetime.timedelta(hours=hour_from_2300)

    #print("server_Time:{}, start_time: {}".format(live1_server_timing, return_time))
    return return_time


# Query SQL to get MT4 Symbol BID/ASK
def get_live2_ask_bid(symbols=[]):


    sql_statement = """SELECT * FROM (
        SELECT LEFT(SYMBOL,6) as SYMBOL, BID, ASK, MODIFY_TIME FROM live2.`mt4_prices`
        where SYMBOL RLIKE '{}'
        AND MODIFY_TIME > now() - INTERVAL 4 DAY
        ORDER BY MODIFY_TIME DESC
        ) as Z 
        GROUP BY SYMBOL""".format("|".join(symbols))

    # Want to get results for the above query, to get the Floating PnL
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result
    # print("result_data:")
    # print(result_data)
    # print("\n")
    result_col = raw_result.keys()  # The column names

    # If empty, we just want to return an empty data frame. So that the following merge will not cause any issues
    return_df = pd.DataFrame(result_data, columns=result_col) if len(result_data) > 0 else pd.DataFrame()

    # Want to clear up the CFDs.
    # Only want to return 1 CFD.
    if len(return_df) > 0 and "SYMBOL" in return_df:
        all_cfds = ['.A50', '.AUS200', '.DE40', '.ES35', '.F40', '.HK50', '.JP225', '.STOXX50', '.UK100', '.UKOil', '.US100', '.US30', '.US500', '.USOil']
        return_df = return_df[ (~ return_df["SYMBOL"].str.contains(".",  regex=False)) | (return_df["SYMBOL"].isin(all_cfds))]
        #print(return_df)

    return return_df


def ABook_LP_Details_function(update_tool_time=0, exclude_list=["demo"]):
                            # LP Details. Balance, Credit, Margin, MC/SO levels. Will alert if email is set to send.
                            # Checks Margin against MC/SO values, with some buffer as alert.

    exclude_conditions = "WHERE LP NOT RLIKE '{}'".format("|".join(exclude_list)) if len(exclude_list) > 0 else ""
    sql_query = text("""SELECT lp, deposit, credit, pnl, equity, total_margin, free_margin,
		ROUND((credit + deposit + pnl),2) as EQUITY,
		COALESCE(ROUND(100 * total_margin / (credit + deposit + pnl),2),0) as `Margin/Equity (%)` ,
		margin_call as `margin_call (M/E)` , stop_out as `stop_out (M/E)`,
			  COALESCE(`stop_out_amount`, ROUND(  100* (`total_margin`/`stop_out`) ,2)) as `STOPOUT AMOUNT`,
		ROUND(`equity` -  COALESCE(`stop_out_amount`, 100* (`total_margin`/`stop_out`) ),2) as `available`,
		updated_time
		FROM aaron.lp_summary {} ORDER BY LP DESC""".format(exclude_conditions))  # Need to convert to Python Friendly Text.
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()
    # result_data_json_parse = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_data_json_parse = [[time_difference_check(a) if isinstance(a, datetime.datetime) else a for a in d] for d in
                           result_data]  # correct The decimal.Decimal class to float.

    result_col = raw_result.keys()
    return_result = [dict(zip(result_col,d)) for d in result_data_json_parse]


    LP_Position_Show_Table = [] # to store the LP details in a more read-able sense

    Tele_Margin_Text = "*LP Margin Issues.*\n"    # To Compose Telegram text for Margin issues.
    margin_attention_flag = 0
    Margin_MC_Flag = 0
    Margin_SO_Flag = 0
    Play_Sound = 0  # For return to AJAX


    for i,lp in enumerate(return_result):    # Want to re-arrange.
        loop_buffer = dict()
        loop_buffer["LP"] = lp["lp"] if "lp" in lp else None

        Lp_MC_Level = lp["margin_call (M/E)"] if "margin_call (M/E)" in lp else None
        Lp_SO_Level = lp["stop_out (M/E)"] if "stop_out (M/E)" in lp else None
        Lp_Margin_Level = lp["Margin/Equity (%)"]  if "Margin/Equity (%)" in lp else 0

        # Want to induce an error
        #Lp_Margin_Level = lp["Margin/Equity (%)"] + 105  if "Margin/Equity (%)" in lp else None

        loop_buffer["BALANCE"] = dict()
        loop_buffer["BALANCE"]["DEPOSIT"] = "$ {:,.2f}".format(float(lp["deposit"])) if "deposit" in lp else None
        loop_buffer["BALANCE"]["CREDIT"] = "$ {:,.2f}".format(float(lp["credit"])) if "credit" in lp else None



        loop_buffer["BALANCE"]["PNL"] = "$ {}".format(profit_red_green(float(lp["pnl"]))) if "pnl" in lp else None
        loop_buffer["BALANCE"]["EQUITY"] = "$ {:,.2f}".format(float(lp["equity"])) if "equity" in lp else None

        loop_buffer["MARGIN"] = dict()
        loop_buffer["MARGIN"]["TOTAL_MARGIN"] = "${:,.2f}".format(float(lp["total_margin"])) if "total_margin" in lp else None
        loop_buffer["MARGIN"]["FREE_MARGIN"] = "${:,.2f}".format(float(lp["free_margin"])) if "free_margin" in lp else None

        # Checking Margin Levels.
        if Lp_Margin_Level >= (Lp_SO_Level - LP_MARGIN_ALERT_LEVEL):    # Check SO Level First.
            Margin_SO_Flag +=1
            loop_buffer["MARGIN/EQUITY (%)"] = "SO Alert: {:.2f}%".format(Lp_Margin_Level)
            Tele_Margin_Text += "_SO Alert_: {} margin is at {:.2f}.\n".format( loop_buffer["LP"], Lp_Margin_Level)
        elif Lp_Margin_Level >= Lp_MC_Level:                            # Check Margin Call Level
            Margin_MC_Flag += 1
            loop_buffer["MARGIN/EQUITY (%)"] = "Margin Call: {:.2f}%".format(Lp_Margin_Level)
            Tele_Margin_Text += "_MC Alert_: {} margin is at {}. MC/SO: {:.2f}/{:.2f}\n".format(loop_buffer["LP"], Lp_Margin_Level,Lp_MC_Level,Lp_SO_Level)
        elif Lp_Margin_Level >= (Lp_MC_Level - LP_MARGIN_ALERT_LEVEL):  # Want to start an alert when reaching MC.
            margin_attention_flag += 1
            loop_buffer["MARGIN/EQUITY (%)"] = "Alert: {:.2f}%".format(Lp_Margin_Level)
            Tele_Margin_Text += "_Margin Alert_: {} margin is at {}. MC is at {:.2f}\n".format(loop_buffer["LP"], Lp_Margin_Level, Lp_MC_Level)
        else:
            loop_buffer["MARGIN/EQUITY (%)"] = "{}%".format(Lp_Margin_Level)

        loop_buffer["MC/SO/AVAILABLE"] = dict()
        loop_buffer["MC/SO/AVAILABLE"]["MARGIN_CALL (M/E)"] = "{:.2f}".format(Lp_MC_Level)
        loop_buffer["MC/SO/AVAILABLE"]["STOP_OUT (M/E)"] = "{:.2f}".format(Lp_SO_Level)
        loop_buffer["MC/SO/AVAILABLE"]["STOPOUT AMOUNT"] = "$ {:,.2f}".format(float(lp["STOPOUT AMOUNT"])) if "STOPOUT AMOUNT" in lp else None
        loop_buffer["MC/SO/AVAILABLE"]["AVAILABLE"] = "$ {:,.2f}".format(float(lp["available"])) if "available" in lp else None

        loop_buffer["UPDATED_TIME"] = lp["updated_time"] if "updated_time" in lp else None

        LP_Position_Show_Table.append(loop_buffer)
        # print(loop_buffer)

    if request.method == 'POST':
        post_data = dict(request.form)
        #print(post_data)

        # Get variables from POST.
        Send_Email_Flag = int(post_data["send_email_flag"]) if ("send_email_flag" in post_data) \
                               and (isinstance(post_data['send_email_flag'], str)) else 0

        lp_attention_email_count = int(post_data["lp_attention_email_count"]) if ("lp_attention_email_count" in post_data) \
                                and (isinstance(post_data['lp_attention_email_count'], str)) else 0

        lp_mc_email_count = int(post_data["lp_mc_email_count"]) if ("lp_mc_email_count" in post_data) \
                                 and (isinstance(post_data['lp_mc_email_count'], str)) else 0


        lp_time_issue_count = int(post_data["lp_time_issue_count"]) if ("lp_time_issue_count" in post_data) \
                                 and (isinstance(post_data['lp_time_issue_count'], str)) else -1


        lp_so_email_count = int(post_data["lp_so_email_count"]) if ("lp_so_email_count" in post_data) \
                                and (isinstance(post_data['lp_so_email_count'], str)) else -1



        if Send_Email_Flag == 1:    # Want to update the runtime table to ensure that tool is running.
            async_update_Runtime(app=current_app._get_current_object(), Tool="LP_Details_Check")


        Tele_Message = "*LP Details* \n"  # To compose Telegram outgoing message

        # Checking if there are any update time that are slow. Returns a Bool
        update_time_slow = any([[True for a in d if (isinstance(a, datetime.datetime) and abs((a-datetime.datetime.now()).total_seconds()) > TIME_UPDATE_SLOW_MIN*60)] for d in result_data])


        if update_time_slow: # Want to send an email out if time is slow.
            if lp_time_issue_count == 0:    # For time issue.
                if Send_Email_Flag == 1:
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])

                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject="LP Details not updating",
                                     HTML_Text="{}Hi,<br><br>LP Details not updating. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".format(
                                         Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,Email_Footer), Attachment_Name=[])


                    LP_Issue_Name = [d['lp'] for d in result_data if ("lp" in d) and ("updated_time" in d) and abs(
                        (d["updated_time"] - datetime.datetime.now()).total_seconds()) > (TIME_UPDATE_SLOW_MIN * 60)]
                    Tele_Message += "_Update Slow_: {}\n".format(", ".join(LP_Issue_Name))

                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Message, TELE_CLIENT_ID)

                Play_Sound+=1   # No matter sending emails or not, we will need to play sound.
                lp_time_issue_count += 1
        else:   # Reset to 0.
            lp_time_issue_count = 0

        #Margin_SO_Flag: 0, Send_Email_Flag: 1, lp_so_email_count: 0, Margin_MC_Flag: 2

        #print("Margin_SO_Flag : {}, lp_so_email_count: {}, Send_Email_Flag:{}".format(Margin_SO_Flag, lp_so_email_count, Send_Email_Flag))
        #print("Margin_MC_Flag : {}, lp_mc_email_count: {}, Send_Email_Flag:{}".format(Margin_MC_Flag, lp_mc_email_count, Send_Email_Flag))
        #print("margin_attention_flag : {}, lp_attention_email_count: {}, Send_Email_Flag:{}".format(margin_attention_flag, lp_attention_email_count, Send_Email_Flag))

        # -------------------- To Check the Margin Levels, and to send email when needed. --------------------------
        if Margin_SO_Flag > 0:   # If there are margin issues. Want to send Alert Out.
            if lp_so_email_count == 0:
                if Send_Email_Flag == 1:
                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Margin_Text, TELE_CLIENT_ID)
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])


                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject = "LP Account approaching SO.",
                                     HTML_Text="{}Hi,<br><br>LP Account margin reaching SO Levels. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".
                                     format(Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Email_Footer),
                                     Attachment_Name=[])


            Play_Sound += 1  # No matter sending emails or not, we will need to play sound.
            # print("Play Sound: {}".format(Play_Sound))
            lp_so_email_count += 1
            lp_mc_email_count = lp_mc_email_count + 1 if lp_mc_email_count <1 else lp_mc_email_count    # Want to raise all to at least 1
            margin_attention_flag = margin_attention_flag + 1 if margin_attention_flag < 1 else margin_attention_flag
        elif Margin_MC_Flag > 0:   # If there are margin issues. Want to send Alert Out.
            if lp_mc_email_count == 0:
                if Send_Email_Flag == 1:
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])

                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject = "LP Account has passed MC Levels.",
                                     HTML_Text="{}Hi,<br><br>LP Account margin reaching SO Levels. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".
                                     format(Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Email_Footer),
                                     Attachment_Name=[])


                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Margin_Text, TELE_CLIENT_ID)
                Play_Sound += 1             # Play sound when MC. Once
            lp_mc_email_count += 1
            lp_so_email_count = 0
        elif margin_attention_flag > 0:  # If there are margin issues. Want to send Alert Out
            if lp_attention_email_count == 0:
                if Send_Email_Flag == 1:
                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Margin_Text, TELE_CLIENT_ID)
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])

                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject = "LP Account approaching MC.",
                                     HTML_Text="{}Hi,<br><br>LP Account margin reaching SO Levels. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".
                                     format(Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Email_Footer),
                                     Attachment_Name=[])

                Play_Sound += 1              # Play sound when Nearing MC. Once
            lp_attention_email_count += 1
            lp_so_email_count = 0
            lp_mc_email_count = 0

        else:       # Clear all flags.
            lp_so_email_count = 0
            lp_mc_email_count = 0
            lp_attention_email_count = 0
       # , "Alert", "Margin Call", "SO Attention"

            # return "Error:lalalala"
    return {"lp_attention_email_count": lp_attention_email_count, "Play_Sound": Play_Sound, "current_result": LP_Position_Show_Table, "lp_time_issue_count": lp_time_issue_count,
                       "lp_so_email_count": lp_so_email_count, "lp_mc_email_count": lp_mc_email_count}


# Helper function to do a time check.
# Return "Update Slow<br> time" if it is more then 10 mins difference.
def time_difference_check(time_to_check):
    time_now = datetime.datetime.now()
    #time_now = datetime.datetime.now()  # Get the time now.
    if not isinstance(time_to_check, datetime.datetime):
        return "Error: Not datetime.datetime object"

    if abs((time_now - time_to_check).total_seconds()) > TIME_UPDATE_SLOW_MIN*60: # set the update to 10 mins.
        return time_to_check.strftime("<b>Update Slow</b><br>%Y-%m-%d<br>%H:%M:%S")
    else:
        return time_to_check.strftime("%Y-%m-%d<br>%H:%M:%S")


# withthe option to return the dataframe
def color_profit_for_df(data, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=False):

    df = pd.DataFrame(data=data) if len(data) != 0 else pd.DataFrame(data=default)
    col_to_change = [c for c in df.columns if max([c.lower().find(w) >= 0 for w in words_to_find]) ] if len(words_to_find) > 0 else []
    # Want to change to color if there are profit, showing green and red for profit and loss respectively.
    for c in col_to_change:
        df[c] = df[c].apply(lambda x: profit_red_green(x))

    # Only if we want to return the dataframe
    if return_df:
        return df

    ret_val = df.to_dict("record")

    return ret_val


# Count how many days backwards to get the num number of weekdays
def count_weekday_backwards(num):
    ret_count = 0

    day_today = datetime.datetime.now().weekday()

    while num > 0:
        ret_count += 1
        if day_today in (0, 1, 2, 3, 4):
            num = num - 1
        # Go back 1 weekday. +_6 and Mod 7 to ensure it will not be negative.
        day_today = (day_today + 6) % 7


    return ret_count

# Split a list into n parts
def split_list_n_parts(data_list, n):
    #n =  How many to split it into
    split_list = []  # To save all the unsync Results

    for i in range(n):
        start_index = i * math.floor(len(data_list) / n)

        # Because of how it might be unevenly divided, we will use the last one as the one with the most.
        end_index = min((i + 1) * math.floor(len(data_list) / n), len(data_list) - 1) if i != n-1 else len(data_list) -1


        #print("length of data_list: {} | {} : {}".format(len(data_list), start_index,end_index))
        split_list.append(data_list[start_index:end_index])
    return split_list


def get_symbol_digits(symbol):
    sql_query = "SELECT SYMBOL, DIGITS FROM live1.mt4_symbols WHERE Symbol like '{}'".format(symbol)
    return query_SQL_return_record(sql_query)


# Check the df, want to count the datetime.
# We only want the datetime that is more common. > 3
def clear_df_datetime(df_original, counter_limit = 3):
    df = df_original.copy() # Make a copy so we won't overwrite the original
    # Want to get rid of the datetime that only appears once.
    df["counter_buffer"] = 1
    df_datetime_count = df[["DATETIME", "counter_buffer"]].groupby("DATETIME").count().reset_index()
    # Want to have at least 3 counters, else, it could possibily be a rouge time
    df_datetime_count = df_datetime_count[df_datetime_count["counter_buffer"] > counter_limit]
    return df_datetime_count["DATETIME"].to_list()

# make the list of dicts printable for flask
# input should be a list of dicts
def flask_printable_list(l):
    return_list = []
    for dict_loop in l:
        dict_buff = {} # Dict buffer
        for k,d in dict_loop.items():
            # If it's a datetime, want to change it to a string
            if (type(d) == datetime.datetime) | (type(d) == pd.Timestamp):
                d = d.strftime("%Y-%m-%d %H:%M:%S")

            dict_buff[k] = d

        return_list.append(dict_buff)
    return return_list
