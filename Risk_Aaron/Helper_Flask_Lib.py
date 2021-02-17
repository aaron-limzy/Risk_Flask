
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db

from flask_table import create_table, Col
from flask import url_for
import decimal
from Aaron_Lib import *
import pandas as pd
from unsync import unsync


if get_machine_ip_address() == '192.168.64.73': #Only On Server computer
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com", "Risk@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com", "risk@blackwellglobal.com", "cs@bgifx.com"]
    print("On Server 64.73")
else:
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com"]
    print("On Aaron's Computer")

EMAIL_AARON =  ["aaron.lim@blackwellglobal.com"]     # For test Groups.
EMAIL_LIST_RISKTW = ["aaron.lim@blackwellglobal.com", "fei.shao@blackwellglobal.com", "nicole.cheng@blackwellglobal.com"]



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

    # [res, col] = Query_SQL(SQL_Query)

    day_backwards = "{}".format(get_working_day_date(datetime.date.today(), -1 * day_backwards_count))


    SQL_Query_Volume_Days = """SELECT {select_column} 
        FROM aaron.bgi_float_history_past 
        WHERE datetime >= '{day_backwards}'
        {symbol_condition}       
        AND COUNTRY IN (SELECT COUNTRY from live5.group_table where BOOK = "{book}") 
        {country_condition} """.format(select_column=select_column, symbol=symbol, book=book,
                                       country_condition=country_condition,
                                       symbol_condition=symbol_condition, day_backwards=day_backwards)


    SQL_Volume_Query = " UNION ".join([SQL_Query_Volume_Recent, SQL_Query_Volume_Days])
    #
    SQL_Volume_Query = SQL_Volume_Query.replace("\n", " ").replace("\t", " ")

    # Use the unsync version to query SQL
    results = unsync_query_SQL_return_record(SQL_Volume_Query, app)
    df_data_vol = pd.DataFrame(results)
    #print(df_data_vol)

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
        cfd_list = [".A50", ".AUS200", ".DE30", ".ES35", ".F40", ".HK50", ".JP225",
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


