
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db
from Helper_Flask_Lib import check_session_live1_timing
from flask_table import create_table, Col
from flask import url_for, current_app, session
import decimal
from Aaron_Lib import *
from app.mt5_queries.mt5_sql_queries import *
import pandas as pd
from unsync import unsync



# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_mt5_db_engine(sql_query):

    raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'mt5_live1'))
    #raw_result = db5.engine.execute(text(sql_query))

    #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
    #return raw_result

    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results



def SQL_insert_MT5(header="", values = [" "], footer = "", sql_max_insert=500):

    for i in range(math.ceil(len(values) / sql_max_insert)):
        # To construct the sql statement. header + values + footer.
        sql_trades_insert = header + " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]) + footer
        sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
        #print(sql_trades_insert)
        sql_trades_insert = text(sql_trades_insert)  # To make it to SQL friendly text.
        raw_insert_result = db.session.execute(sql_trades_insert,  bind=db.get_engine(current_app, 'mt5_live1'))
        db.session.commit()
    return


def SQL_insert_MT5_statement(sql_insert):

    # To make it to SQL friendly text.
    raw_insert_result = db.session.execute( text(sql_insert),  bind=db.get_engine(current_app, 'mt5_live1'))
    #print(raw_insert_result.fetchall())
    #print(sql_insert)
    db.session.commit() # Since we are using session, we need to commit.
    return


# Want to get the MT5 PnL By Symbol from Yudi's Database.

def mt5_yesterday_symbol_pnl():
    sql_query = mt5_symbol_yesterday_pnl_query()
    result = Query_SQL_mt5_db_engine(sql_query)
    #print(result)
    return result


# This function will return combined data of symbol float as well as yesterday's PnL for MT5
def mt5_symbol_float_data():
    Testing = False

    # Want to get PnL For MT5 Yesterday.
    if check_session_live1_timing() == True and "yesterday_mt5_pnl_by_symbol" in session \
            and len(session["yesterday_mt5_pnl_by_symbol"]) > 0:
        # From "in memory" of session
        # print(session)
        df_yesterday_symbol_pnl = pd.DataFrame.from_dict(session["yesterday_mt5_pnl_by_symbol"])
    else:  # If session timing is outdated, or needs to be updated.

        print("\nGetting yesterday MT5 symbol PnL from DB\n\n")

        df_yesterday_symbol_pnl = pd.DataFrame(mt5_yesterday_symbol_pnl())
        if "DATE" in df_yesterday_symbol_pnl:  # We want to save it as a string.
            df_yesterday_symbol_pnl['Date'] = df_yesterday_symbol_pnl['DATE'].apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime.date) else x)
        # save it to session


        session["yesterday_mt5_pnl_by_symbol"] = df_yesterday_symbol_pnl.to_dict()

    # To simulate if there was no closed symbol on MT5 yesterday
    #df_yesterday_symbol_pnl = pd.DataFrame([])

    if Testing == True:
        test_data = [ {'SYMBOL': 'BTCUSD', 'YesterdayVolume': 0.26, 'YesterdayProfitUsd': 2.62, 'YesterdayRebate': 0.0,
          'DATETIME': datetime.datetime(2021, 3, 22, 14, 20, 17)},
                   {'SYMBOL': 'EURUSD', 'YesterdayVolume': 0.26, 'YesterdayProfitUsd': 2.62, 'YesterdayRebate': 0.0,
                    'DATETIME': datetime.datetime(2021, 3, 22, 14, 20, 17)},
                   {'SYMBOL': 'XAUUSD', 'YesterdayVolume': 0.26, 'YesterdayProfitUsd': 2.62, 'YesterdayRebate': 0.0,
                    'DATETIME': datetime.datetime(2021, 3, 22, 14, 20, 17)} ]
        df_yesterday_symbol_pnl =  pd.DataFrame.from_dict(test_data)

    # Want to rename the columns
    df_yesterday_symbol_pnl.rename(columns={"YesterdayProfitUsd":"YESTERDAY_REVENUE", 'YesterdayRebate': "YESTERDAY_REBATE", "YesterdayVolume":"YESTERDAY_LOTS"}, inplace=True)

    #print(df_yesterday_symbol_pnl.to_dict())


    if Testing == True:
        result_data = [{'SYMBOL': 'BTCUSD', 'NET_LOTS': -0.2, 'FLOATING_LOTS': 0.4, 'REVENUE': -3725.73, 'TODAY_LOTS': -14.72, 'TODAY_REVENUE': 0.13, 'DATETIME': datetime.datetime(2021, 3, 22, 14, 49, 11)},
                       {'SYMBOL': 'XAUUSD', 'NET_LOTS': -0.2, 'FLOATING_LOTS': 0.4, 'REVENUE': 725.73, 'TODAY_LOTS': -14.72, 'TODAY_REVENUE': 0.13, 'DATETIME': datetime.datetime(2021, 3, 22, 14, 49, 11)},
                       {'SYMBOL': 'NOTASYMBOL', 'NET_LOTS': -0.2, 'FLOATING_LOTS': 0.4, 'REVENUE': -35.73, 'TODAY_LOTS': -14.72, 'TODAY_REVENUE': 0.13, 'DATETIME': datetime.datetime(2021, 3, 22, 14, 49, 11)},
                       {'SYMBOL': 'LALALALA', 'NET_LOTS': -0.2, 'FLOATING_LOTS': 0.4, 'REVENUE': 3725.73, 'TODAY_LOTS': -14.72, 'TODAY_REVENUE': 0.13, 'DATETIME': datetime.datetime(2021, 3, 22, 14, 49, 11)},
                       {'SYMBOL': 'BTCUSD', 'NET_LOTS': -0.2, 'FLOATING_LOTS': 0.4, 'REVENUE': 12345.73, 'TODAY_LOTS': -14.72, 'TODAY_REVENUE': 0.13, 'DATETIME': datetime.datetime(2021, 3, 22, 14, 49, 11)}]
    else:
        # Now to get the Symbol float and today's closed.

        sql_query = """SELECT SYMBOL, ROUND(SUM(NET_FLOATING_VOLUME),2) as `NET_LOTS`, ROUND(SUM(FLOATING_VOLUME),2) as `FLOATING_LOTS`, ROUND(SUM(FLOATING_REVENUE),2) as `REVENUE`, 
            ROUND(SUM(CLOSED_VOL_TODAY),2) as `TODAY_LOTS`,
            ROUND(SUM(CLOSED_REVENUE_TODAY),2) as `TODAY_REVENUE`,  DATETIME
                FROM  aaron.bgi_mt5_float_save
                WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.bgi_mt5_float_save)
                    GROUP BY SYMBOL
                ORDER BY floating_revenue DESC
            """
        result_data = Query_SQL_mt5_db_engine(sql_query)
        #result_data = []

    #print(result_data)
    df = pd.DataFrame(result_data)


    # If either one of them are empty.
    if len(df_yesterday_symbol_pnl) == 0 or len(df) == 0:
        if len(df_yesterday_symbol_pnl) != 0 and len(df) == 0:
            return df_yesterday_symbol_pnl
        elif len(df_yesterday_symbol_pnl) == 0 and len(df) != 0:
            return df
        else:
            return pd.DataFrame([])

    df_combined = df_yesterday_symbol_pnl.merge(df, how='outer', on='SYMBOL')
    #df_combined.fillna("-", inplace=True)
    #print(df)

    return df_combined