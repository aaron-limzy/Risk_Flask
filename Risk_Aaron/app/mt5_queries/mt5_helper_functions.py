
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db
from Helper_Flask_Lib import check_session_live1_timing, profit_red_green
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


# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def unsync_Query_SQL_mt5_db_engine(app_unsync, sql_query, date_to_str=True):

    results = [{}]
    with app_unsync.app_context():  # Need to use original app as this is in the thread

        raw_result = db.session.execute(text(sql_query), bind=db.get_engine(app_unsync, 'mt5_live1'))
        result_data = raw_result.fetchall()

        # If we need to change date to string
        if date_to_str:
            result_data = [["{}".format(a) if isinstance(a, datetime.datetime) else a for a in d] for d in
                               result_data]

        result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d] for d in
                               result_data]  # correct The decimal.Decimal class to float.

        result_col = raw_result.keys()
        results = [dict(zip(result_col, d)) for d in result_data_decimal]

        #db.session.close()

    return results




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


# Want to get the Future's LP details
def mt5_futures_LP_data():
    sql_query = mt5_futures_LP_details_query()
    result = Query_SQL_mt5_db_engine(sql_query)

    #ACCOUNT, CURRENCY, BALANCE, EQUITY, CANDRAW, MARKETEQUITY, ACCTINITIALMARGIN, ACCTMAINTENANCEMARGIN, FROZENFEE, DATETIME
    #print(result)
    return result

# Want to getMT5 A Book Data
def mt5_ABook_data():
    sql_query = mt5_ABook_query()
    result = Query_SQL_mt5_db_engine(sql_query)
    return result



# Want to get the MT5 PnL By Symbol from Yudi's Database.

@unsync
def mt5_HK_ABook_data(unsync_app):
    sql_query = mt5_HK_ABook_query()
    result = unsync_Query_SQL_mt5_db_engine(unsync_app,sql_query)
    #print(result)
    return result


@unsync
def mt5_HK_CopyTrade_Futures_LP_data(unsync_app):
    sql_query = mt5_HK_CopyTrade_Future_query()
    result = unsync_Query_SQL_mt5_db_engine(unsync_app,sql_query)
    #print(result)
    return result


@unsync
def mt5_Query_SQL_mt5_db_engine_query(unsync_app, SQL_Query):
    result = unsync_Query_SQL_mt5_db_engine(unsync_app, SQL_Query)
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





# To print the MT5 LP details into nice structures.
# Will be a table in a table for display on HTML
def pretty_print_mt5_futures_LP_details(df):
    #print(df)
    # If it's an empty dataframe
    if len(df) == 0:
        return df

    if 'DATETIME' in df:
        df['DATETIME'] =  df['DATETIME'].apply(lambda x : "{}".format(x))

    # Need to check if all the columns are in the df
    if all([c in df for c in ["EQUITY", "BALANCE"]]):
        df["PnL"] = df['EQUITY'] -  df['BALANCE']
        # Want to color the profit column
        df["PnL"] =  df["PnL"].apply(profit_red_green)
    else:
        print("Missing Column from df in 'pretty print mt5 futures lp details': {}".format([c for c in ["EQUITY", "BALANCE"] if c not in df]))
        df["PnL"] = 0

    cols = ['EQUITY', 'CANDRAW', "ACCTINITIALMARGIN", "BALANCE", "ACCTMAINTENANCEMARGIN", "FROZENFEE", "MARKETEQUITY"]
    for c in cols:
        if c in df:
            df[c] = df[c].apply(lambda x: "{:,.2f}".format(x))


    # To save some space.
    # Will display as a table inside a table on the page.
    df["BALANCES"] = df.apply(lambda x: {"BALANCE" : x['BALANCE'], 'EQUITY' : x['EQUITY'], "PnL":  x['PnL']} , axis=1)


    #ACCOUNT, CURRENCY, BALANCE, EQUITY, CANDRAW, MARKETEQUITY, ACCTINITIALMARGIN, ACCTMAINTENANCEMARGIN, FROZENFEE, DATETIME
    df.rename(columns={"ACCTINITIALMARGIN" : "ACCT INITIAL MARGIN",
               "ACCTMAINTENANCEMARGIN" : "ACCT MAINTENANCE MARGIN",
                       "FROZENFEE" : "FROZEN FEE"}, inplace=True)

    #print(df.columns)
    cols_to_display = ['ACCOUNT', 'CURRENCY', 'BALANCES' , 'ACCT INITIAL MARGIN', 'ACCT MAINTENANCE MARGIN', 'FROZEN FEE', 'DATETIME']

    cols_to_display = [c for c in cols_to_display if c in df]
    return df[cols_to_display]

# To print the Futures LP details in the same structure as the LP MT4 Details
def pretty_print_mt5_futures_LP_details_2(futures_data, fx_lp_details, return_df=False):


    mt5_hk_LP_Copy_futures_data_df = pd.DataFrame(futures_data)
    #print(mt5_hk_LP_Copy_futures_data_df)

    mt5_hk_LP_Copy_futures_data_df.rename(columns={"DATETIME": "UPDATED_TIME"}, inplace=True)
    # To write as new line.
    mt5_hk_LP_Copy_futures_data_df["UPDATED_TIME"] = mt5_hk_LP_Copy_futures_data_df["UPDATED_TIME"].apply(lambda x: x.replace(" ", "<br>"))
    # Need to check if all the columns are in the df
    if all([c in mt5_hk_LP_Copy_futures_data_df for c in ["EQUITY", "BALANCE"]]):
        mt5_hk_LP_Copy_futures_data_df["PnL"] = mt5_hk_LP_Copy_futures_data_df['EQUITY'] -  mt5_hk_LP_Copy_futures_data_df['BALANCE']
        # Want to color the profit column
        mt5_hk_LP_Copy_futures_data_df["PnL"] =  mt5_hk_LP_Copy_futures_data_df["PnL"].apply(lambda x: "$ {}".format(profit_red_green(x)))
    else:
        #print("Missing Column from df in 'pretty print mt5 futures lp details': {}".format([c for c in ["EQUITY", "BALANCE"] if c not in mt5_hk_LP_Copy_futures_data_df]))
        mt5_hk_LP_Copy_futures_data_df["PnL"] = "-"

    mt5_hk_LP_Copy_futures_data_df["LP"] = mt5_hk_LP_Copy_futures_data_df.apply(lambda x: "{}_{}".format(x["ACCOUNT"], x["CURRENCY"]), axis=1)

    mt5_hk_LP_Copy_futures_data_df["MC/SO/AVAILABLE"] = "-"
    mt5_hk_LP_Copy_futures_data_df["MARGIN/EQUITY (%)"] = "-"

    # Will display as a table inside a table on the page.
    mt5_hk_LP_Copy_futures_data_df["BALANCE"] = mt5_hk_LP_Copy_futures_data_df.apply(lambda x: \
                    {"DEPOSIT" : "$ {:,.2f}".format(x['BALANCE']),
                     'EQUITY' : "$ {:.2f}".format(x['EQUITY']),
                     "PnL":  x['PnL'],
                     "FROZEN FEE":  "$ {:,.2f}".format(x['FROZENFEE'])} , axis=1)

    # Will display as a table inside a table on the page.
    mt5_hk_LP_Copy_futures_data_df["MARGIN"] = mt5_hk_LP_Copy_futures_data_df.apply(lambda x: \
                   {"ACCT INITIAL MARGIN" : x['ACCTMAINTENANCEMARGIN'], 'ACCT MAINTENANCE MARGIN' : x['ACCTINITIALMARGIN']} , axis=1)

    # mt5_hk_LP_Copy_futures_data_df["MARGIN"] = mt5_hk_LP_Copy_futures_data_df.apply(lambda x: \
    #                {"ACCT INITIAL MARGIN" : 0, 'ACCT MAINTENANCE MARGIN' :0} , axis=1)

    # Remove all the column that we don't need
    for p in ["ACCOUNT", "CURRENCY", "ACCTMAINTENANCEMARGIN", 'ACCTINITIALMARGIN', 'PnL', "FROZENFEE", "EQUITY"]:
        if p in mt5_hk_LP_Copy_futures_data_df:
            mt5_hk_LP_Copy_futures_data_df.pop(p)

    # Want to see if we can add all the details together.
    mt5_hk_LP_Copy_futures_data_df = pd.concat([mt5_hk_LP_Copy_futures_data_df, pd.DataFrame(fx_lp_details)], axis=0)

    col = ["LP", "BALANCE", "MARGIN", "MARGIN/EQUITY (%)", "MC/SO/AVAILABLE", "UPDATED_TIME"]

    if return_df==False: # If we don't want to return the pandas df
        return mt5_hk_LP_Copy_futures_data_df[[c for c in col if c in mt5_hk_LP_Copy_futures_data_df]].to_dict("record")

    return mt5_hk_LP_Copy_futures_data_df[[c for c in col if c in mt5_hk_LP_Copy_futures_data_df]]
