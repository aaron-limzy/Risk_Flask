from flask import Blueprint, render_template, Markup, url_for, request, session, current_app, flash, redirect
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from app.Swaps.A_Utils import *
from sqlalchemy import text
from app.extensions import db, excel

from app.mt5_queries.mt5_sql_queries import *
from app.Symbol_Spread.Spread_helper_functions import *


import plotly
import plotly.graph_objs as go
import plotly.express as px

import pandas as pd
import numpy as np
import json

from app.decorators import roles_required
from app.Plotly.forms import  Live_Login
from app.Plotly.tableau_url import *
#from app.Plotly.table import Client_Trade_Table
from app.Plotly.routes import check_session_live1_timing

from app.background import *
from bs4 import BeautifulSoup

#from app.tableau_embed import *

from app.Risk_Tools_Config import email_flag_fun

import emoji
import flag

from app.Swaps.forms import UploadForm

from flask_table import create_table, Col

import requests

import pyexcel
from werkzeug.utils import secure_filename

from Helper_Flask_Lib import *
from app.Plotly.Client_Trade_Analysis import *

import decimal

Spread_bp = Blueprint('Spread', __name__)


# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@Spread_bp.route('/Symbol_Spread', methods=['GET', 'POST'])
@roles_required()
def symbolSpread():

    title = Markup("Symbol Spread [Daily]")
    header = Markup("<b>Symbol Spread [Daily]</b>")

    description =Markup("""<b>Symbol Spread [Hourly]</b><br><br>
                Calculating the symbol spread using Live 1 q symbols.<br>
                <b>Mark Up</b><br>
                Mark up (If any) has been removed Based on SQ's database.<br><br>
                <b>Page Loading</b><br>
                Page will generally take about 20 seconds to load as there are quite a number of data points.<br><br>
                <b>Graph Controls</b><br>
                - <b>Click Once</b> on the Symbol in the graph legend to remove it.<br>
                - <b>Double Click</b> on the Symbol in the graph legend to isolate it.<br><br>
                <b>Date/Date Time</b><br>
                - Times are based on Live 1 server time.<br>
                - Date, for example, of 23th, means 22nd 2300 - 23 2259<br>
                - Time, for example, at 0600, means 0600 - 0659<br><br>
                Using Database: "sf_test" """)

    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbol_float_trades"), icon="",
                           Table_name={ "Symbol Float": "H1",
                                        "Plot Of all Spread" : "P_Long_0",
                                        "Plot Of Spread 1": "P_Long_1",
                                        "Plot Of Spread 2": "P_Long_2",
                                        "Plot Of Spread 3": "P_Long_3",
                                        "Plot Of Spread 4": "P_Long_4",
                                        "Plot Of Spread 5": "P_Long_5",
                                        "Plot Of Spread 6": "P_Long_6",
                                        },
                           title=title,
                           ajax_url=url_for('Spread.symbol_spread_ajax', _external=True),
                           header=header, ajax_timeout_sec = 250,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"])) #setinterval=60,



# # The Ajax call for the symbols we want to query. B Book.
# @Spread_bp.route('/symbol_spread_ajax', methods=['GET', 'POST'])
# @roles_required()
# def symbol_spread_ajax():
#
#     test = False
#     days_backwards = count_weekday_backwards(12) if not test else count_weekday_backwards(5)
#
#     # Want to get all the tables that are in the DB
#     sql_query = """SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = "BGI_Live2" """
#
#     # Get it from the Ticks DB
#     res = Query_SQL_ticks_db_engine(sql_query)
#
#     # Find the tables postfixed with "ticks"
#     sql_tables = [r['TABLE_NAME'] for r in res if r['TABLE_NAME'].find("ticks") > 0]
#
#     # Get all the symbols that we need.
#     symbols = list(set([s[0:6] for s in sql_tables if s.find(".") == -1]))
#     symbols.sort()
#
#
#     # The actual symbol name.
#     to_query = []
#     for s in symbols:
#         if s + "q_ticks" in sql_tables:
#             to_query.append((s, s + "q_ticks"))
#         if test:    # If we are testing, we want to stop at 5 to save sometime.
#             if len(to_query) >= 5:
#                 break
#         # else:
#         #     print(s)
#
#
#     sql_average = """SELECT DATE_ADD(DATE_TIME, INTERVAL 1 HOUR) as DATE_TIME, '{symbol}' as Symbol, AVG(ASK-BID) as SPREAD
#     FROM BGI_Live2.{table_name}
#     WHERE DATE_TIME > "{date} 23:00:00"
#     GROUP BY LEFT(DATE_ADD(DATE_TIME, INTERVAL 1 HOUR) , 10)"""
#
#     date = (datetime.datetime.now() - datetime.timedelta(days=days_backwards)).strftime("%Y-%m-%d")
#     all_query_list = [sql_average.format(symbol=s, table_name=t, date=date) for (s, t) in to_query]
#
#
#     # Query the DB for the average ticks per day.
#     time_start = datetime.datetime.now()
#
#     # Want to split up the SQL calls to reduce time taken
#     to_split_num = 5 # How many to split it into
#
#     # First, split the Query into n parts.
#     query_split_list = split_list_n_parts(all_query_list, n=to_split_num)
#     # UNION them all together. So now we have n queries.
#     sql_query_list = [" UNION ".join(q).replace("\n", " ") for q in query_split_list]
#     # Does the SQL calls in an unsync manner
#     unsync_results = [unsync_Query_SQL_ticks_db_engine(sql_query=s,
#                                                app_unsync=current_app._get_current_object(),
#                                                    date_to_str=False) for s in sql_query_list]
#     # Wait to get it all back, to sync it.
#     res_list = [pd.DataFrame(r.result()) for r in unsync_results]
#
#
#     #if test:
#     print("Time taken for getting all symbol spread: {}".format((datetime.datetime.now() - time_start).total_seconds()))
#
#
#     #col = [c[0] for c in res[1]]
#     df = pd.concat(res_list)
#     df["DATE_TIME"] = df["DATE_TIME"].apply(lambda x: x.strftime("%Y-%m-%d"))
#
#     if test:
#         print(df)
#
#     sql_digit_query = """SELECT Symbol, Digits FROM LIVE2.MT4_SYMBOLS WHERE SYMBOL IN ({})""".format(",".join([f"'{s}'" for s in symbols]))
#
#     digits_data = Query_SQL_db_engine(sql_digit_query)
#
#     df_digits = pd.DataFrame(digits_data)
#
#
#
#     df["Symbol"] = df["Symbol"].str.upper()
#
#     df = df.merge(df_digits, on="Symbol")
#     df["Spread_Digits"] = df["SPREAD"] * 10 ** df["Digits"]
#
#     if test:
#         print(df_digits)
#
#     df_pivot = pd.pivot_table(df, values="Spread_Digits", index=["Symbol"], columns=["DATE_TIME"]).reset_index()
#
#     if test:
#         print(df_pivot)
#
#     #df_pivot
#
#     df_pivot = np.round(df_pivot, 2)
#
#     df.sort_values(["DATE_TIME", "Symbol"], inplace=True) # First, we need to sort the values
#
#     all_symbol = df["Symbol"].unique().tolist()
#     all_symbol.sort()
#     split_symbol = split_list_n_parts(all_symbol, n=5)
#
#     if test:
#         print(all_symbol)
#         print(len(all_symbol))
#         print(split_symbol)
#
#     return_dict = {}     # The dict that is used for returning, thru JSON
#     return_dict["H1"] = df_pivot.to_dict("record")
#     return_dict["P_Long_0"] =  plot_symbol_Spread(df, "All Symbol Spread", x_axis="DATE_TIME"))
#
#     # Will append to the list to be returned as figures/plots
#     for i in range(len(split_symbol)):
#         return_dict["P_Long_{}".format(i+1)] = plot_symbol_Spread(df[df["Symbol"].isin(split_symbol[i])], "Symbol Spread {}".format(split_symbol[i]), x_axis="DATE_TIME"))
#
#
#     # Return the values as json.
#     # Each item in the returned dict will become a table, or a plot
#     return json.dumps(return_dict, cls=plotly.utils.PlotlyJSONEncoder)


# The Ajax call for the symbols we want to query. B Book.
@Spread_bp.route('/symbol_spread_ajax', methods=['GET', 'POST'])
@roles_required()
def symbol_spread_ajax():

    test = False

    # Query the DB for the average ticks per day.
    time_start = datetime.datetime.now()

    sf_database = "sf_test"
    days_backwards = count_weekday_backwards(12) if not test else count_weekday_backwards(5)

    date = (datetime.datetime.now() - datetime.timedelta(days=days_backwards)).strftime("%Y-%m-%d")

    # Want to get all the tables that are in the DB
    sql_query = """SELECT * FROM {sf_database}.live1q_daily WHERE DATE >= '{date}'""".format(sf_database=sf_database, date=date)

    # Get it from the Ticks DB
    res = unsync_Query_SQL_ticks_db_engine(sql_query=sql_query, app_unsync=current_app._get_current_object(),
                                                       date_to_str=False)

    df = pd.DataFrame(res.result())

    # Change the name of the columns
    df.rename(columns={"AVG_spread": "SPREAD"}, inplace=True)

    # Want to get the actual symbol name.
    df_mt4_symbol = df["TABLE"].apply(lambda x: "'{}'".format(x[x.find(".") + 1 : ].replace("_ticks", "")))
    #print(list(set(df_mt4_symbol)))
    unique_postfix_list = list(set(df_mt4_symbol))

    sql_query_markup = "SELECT SOURCESYMB, POSTFIXSYMB, BIDSPREAD, ASKSPREAD from live1.symbol_o where POSTFIXSYMB in ({})".format(" , ".join(unique_postfix_list))
    #print(sql_query_markup)

    markup_res = Query_Symbol_Markup_db_engine(sql_query_markup)
    df_markup = pd.DataFrame(markup_res)

    # to remove the q postfix for each symbol. So that we can do a join
    df_markup["SYMBOL"] = df_markup["POSTFIXSYMB"].apply(lambda x: x.replace("q", ""))
    #df_markup.rename(columns={"SOURCESYMB": "SYMBOL"}, inplace=True)
    #print(df_markup)


    df = df.merge(df_markup, on="SYMBOL")
    #print(df)
    # Calculate the markup
    df["TOTAL_MARKUP"] = df["ASKSPREAD"] - df["BIDSPREAD"]
    df["SPREAD"] = df["SPREAD"] - df["TOTAL_MARKUP"]

    # Print the dates correctly.
    df["DATE"] = df["DATE"].apply(lambda x: x.strftime("%Y-%m-%d"))

    if test:
        print(df)

    df["SYMBOL"] = df["SYMBOL"].str.upper()
    # # Get all the symbols that we need.
    symbols = df["SYMBOL"].unique().tolist()
    symbols.sort()



    df_pivot = pd.pivot_table(df, values="SPREAD", index=["SYMBOL"], columns=["DATE"]).reset_index()

    if test:
        print(df_pivot)


    df_pivot = np.round(df_pivot, 2)
    df.sort_values(["DATE", "SYMBOL"], inplace=True)  # First, we need to sort the values


    # We don't want to mix the CFDs with the FXs/PMs
    fx_symbol = df[~ df["SYMBOL"].str.startswith(".")]["SYMBOL"].tolist()
    fx_symbol.sort()
    split_symbol = split_list_n_parts(fx_symbol, n=5)

    if test:
        print(symbols)
        print(len(symbols))
        print(split_symbol)

    return_dict = {}  # The dict that is used for returning, thru JSON
    return_dict["H1"] = df_pivot.to_dict("record")
    return_dict["P_Long_0"] = plot_symbol_Spread(df, "All Symbol Spread", x_axis="DATE")
    return_dict["P_Long_1"] = plot_symbol_Spread(df[df["SYMBOL"].str.startswith(".")], "All CFD Spread", x_axis="DATE")


    # Will append to the list to be returned as figures/plots
    for i in range(len(split_symbol)):
        return_dict["P_Long_{}".format(i + 2)] = plot_symbol_Spread(df[df["SYMBOL"].isin(split_symbol[i])],
                                                                    "Symbol Spread {}".format(split_symbol[i]), x_axis="DATE")


    #if test:
    print("Time taken for getting all symbol spread: {}".format((datetime.datetime.now() - time_start).total_seconds()))


    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps(return_dict, cls=plotly.utils.PlotlyJSONEncoder)



# # To Query for all open trades by a particular symbol
# # Shows the closed trades for the day as well.
# @Spread_bp.route('/Symbol_Spread_hourly', methods=['GET', 'POST'])
# @roles_required()
# def symbolSpread_custom_hourly():
#
#     title = Markup("Symbol Spread [Hourly]")
#     header = Markup("<b>Symbol Spread [Hourly]</b>")
#
#     description = Markup("""<b>Symbol Spread [Hourly]</b><br><br>
#                 Calculating the symbol spread using Live 1 q symbols.<br>
#                 <b>Mark Up</b><br>
#                 Mark up (If any) has been removed Based on SQ's database.<br><br>
#                 <b>Page Loading</b><br>
#                 Page will generally take about 20 seconds to load as there are quite a number of data points.<br><br>
#                 <b>Graph Controls</b><br>
#                 - <b>Click Once</b> on the Symbol in the graph legend to remove it.<br>
#                 - <b>Double Click</b> on the Symbol in the graph legend to isolate it.<br><br>
#                 <b>Date/Date Time</b><br>
#                 - Times are based on Live 1 server time.<br>
#                 - Date, for example, of 23th, means 22nd 2300 - 23 2259<br>
#                 - Time, for example, at 0600, means 0600 - 0659<br><br>
#                 Using Database: "sf_test" """)
#
#     return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbol_float_trades"), icon="",
#                            Table_name={ "Symbol Float": "H_y_scroll_1",
#                                         "Plot Of all Spread" : "P_Long_0",
#                                         "Plot Of Spread 1": "P_Long_1",
#                                         "Plot Of Spread 2": "P_Long_2",
#                                         "Plot Of Spread 3": "P_Long_3",
#                                         "Plot Of Spread 4": "P_Long_4",
#                                         "Plot Of Spread 5": "P_Long_5",
#                                         "Plot Of Spread 6": "P_Long_6",
#                                         },
#                            title=title,
#                            ajax_url=url_for('Spread.symbol_spread_custom_ajax', type="hourly",  _external=True),
#                            header=header, ajax_timeout_sec = 250,
#                            description=description, no_backgroud_Cover=True,
#                            replace_words=Markup(["Today"])) #setinterval=60,




# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@Spread_bp.route('/Symbol_Spread/<type>', methods=['GET', 'POST'])
@roles_required()
def symbolSpread_custom(type):

    title = Markup("Symbol Spread [{}]".format(type))
    header = Markup("<b>Symbol Spread [{}]</b>".format(type))

    description = Markup("""<b>Symbol Spread [{}]</b><br><br>
                Calculating the symbol spread using Live 1 q symbols.<br><br>
                <b>Mark Up</b><br>
                Mark up (If any) has been removed Based on SQ's current database. (no back tracking)<br><br>
                <b>Page Loading</b><br>
                Page will generally take about 20 seconds to load as there are quite a number of data points.<br><br>
                <b>Graph Controls</b><br>
                - <b>Click Once</b> on the Symbol in the graph legend to remove it.<br>
                - <b>Double Click</b> on the Symbol in the graph legend to isolate it.<br><br>
                <b>Date/Date Time</b><br>
                - Times are based on Live 1 server time.<br>
                - Date, for example, of 23th, means 22nd 2300 - 23 2259<br>
                - Time, for example, at 0600, means 0600 - 0659<br><br>
                Using Database: "sf_test" """.format(type))

    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbolspread_{}".format(type)), icon="",
                           Table_name={ "Symbol Float": "H_y_scroll_1",
                                        "Plot Of all Spread" : "P_Long_0",
                                        "Plot Of Spread 1": "P_Long_1",
                                        "Plot Of Spread 2": "P_Long_2",
                                        "Plot Of Spread 3": "P_Long_3",
                                        "Plot Of Spread 4": "P_Long_4",
                                        "Plot Of Spread 5": "P_Long_5",
                                        "Plot Of Spread 6": "P_Long_6",
                                        "Plot Of Spread 7": "P_Long_7",
                                        },
                           title=title,
                           ajax_url=url_for('Spread.symbol_spread_custom_ajax', type=type.lower(),  _external=True),
                           header=header, ajax_timeout_sec = 250,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"])) #setinterval=60,






# The Ajax call for the symbols we want to query. B Book.
@Spread_bp.route('/symbol_spread_custom_ajax/<type>', methods=['GET', 'POST'])
@roles_required()
def symbol_spread_custom_ajax(type="hourly"):

    test = False

    # Query the DB for the average ticks per day.
    time_start = datetime.datetime.now()

    if type.lower() == "daily":
        sf_database = "sf_test"
        table_name = "live1q_daily"
    elif type.lower() == "hourly":
        sf_database = "sf_test"
        table_name = "live1q_hourly"
    else:
        return json.dumps({"H_y_scroll_1": [{"Error": "Not 'daily' or 'hourly'"}]}, cls=plotly.utils.PlotlyJSONEncoder)

    days_backwards = count_weekday_backwards(12) if not test else count_weekday_backwards(5)

    date = (datetime.datetime.now() - datetime.timedelta(days=days_backwards)).strftime("%Y-%m-%d")

    # Want to get all the tables that are in the DB
    sql_query = """SELECT * FROM {sf_database}.{table_name} WHERE DATE >= '{date}'""".format(sf_database=sf_database, date=date, table_name=table_name)

    # Get it from the Ticks DB
    res = unsync_Query_SQL_ticks_db_engine(sql_query=sql_query,
                                           app_unsync=current_app._get_current_object(),
                                                       date_to_str=False)

    df = pd.DataFrame(res.result())

    # Change the name of the columns
    df.rename(columns={"AVG_spread": "SPREAD", "AVG": "SPREAD"}, inplace=True)



    # Want to get the actual symbol name.
    df_mt4_symbol = df["SYMBOL"].apply(lambda x: "'{}q'".format(x))
    #print(list(set(df_mt4_symbol)))
    unique_postfix_list = list(set(df_mt4_symbol))

    sql_query_markup = "SELECT SOURCESYMB, POSTFIXSYMB, BIDSPREAD, ASKSPREAD from live1.symbol_o where POSTFIXSYMB in ({})".format(" , ".join(unique_postfix_list))
    #print(sql_query_markup)

    markup_res = Query_Symbol_Markup_db_engine(sql_query_markup)
    df_markup = pd.DataFrame(markup_res)



    # to remove the q postfix for each symbol. So that we can do a join
    df_markup["SYMBOL"] = df_markup["POSTFIXSYMB"].apply(lambda x: x.replace("q", ""))
    #df_markup.rename(columns={"SOURCESYMB": "SYMBOL"}, inplace=True)
    #print(df_markup)


    df = df.merge(df_markup, on="SYMBOL")



    #print(df)
    # Calculate the markup
    df["TOTAL_MARKUP"] = df["ASKSPREAD"] - df["BIDSPREAD"]
    df["SPREAD"] = df["SPREAD"] - df["TOTAL_MARKUP"]




    if test:
        print(df)

    df["SYMBOL"] = df["SYMBOL"].str.upper()
    # # Get all the symbols that we need.
    symbols = df["SYMBOL"].unique().tolist()
    symbols.sort()

    date_col = ""
    # Check if the columns are in.
    if all([c in df for c in ["DATE", "HOUR"]]):
        # Append the hour to the date. Making it a datetime.
        df["DATE_TIME"] = df.apply(lambda x: x["DATE"] + pd.DateOffset(hours=x["HOUR"]), axis=1)
        date_col = "DATE_TIME"
    elif "DATE" in df:
        # Print the dates correctly.
        date_col = "DATE"


    # Want to print hte dates into string
    for c in df.columns:
        if c.lower().find("date") >=0 :
            df[c] = df[c].apply(lambda x: "{}".format(x))
            #df[c] = df[c].apply(lambda x: x.strftime("%Y-%m-%d"))



    df_pivot = pd.pivot_table(df, values="SPREAD", index=["SYMBOL"], columns=[date_col]).reset_index()
    df_pivot.fillna("-", inplace=True)


    if test:
        print(df_pivot)


    # print(df_pivot)
    #print(df)

    # Computing IQR. Want to split out some high spread symbols so that the chart looks better.
    df_grouped_max = df.groupby("SYMBOL").max().reset_index()
    df_grouped_max.rename(columns={"AVG_spread": "SPREAD", "AVG": "SPREAD"}, inplace=True)

    Q1 = df_grouped_max['SPREAD'].quantile(0.25)
    Q3 = df_grouped_max['SPREAD'].quantile(0.75)
    IQR = Q3 - Q1
    Upper_limit = Q3 + 4 * IQR
    Upper_limit_Symbols = df_grouped_max[df_grouped_max["SPREAD"] >= Upper_limit]["SYMBOL"].unique().tolist()
    # print("IQR : {}, Q3 + 4*IQR: {}".format(IQR, Upper_limit))
    # print("Symbols above 1.5IQR + Q3: {}".format(Upper_limit_Symbols))


    df_pivot = np.round(df_pivot, 2)
    df.sort_values([date_col, "SYMBOL"], inplace=True)  # First, we need to sort the values


    # We don't want to mix the CFDs with the FXs/PMs
    fx_symbol = df[~ df["SYMBOL"].str.startswith(".")]["SYMBOL"].tolist()
    fx_symbol = [x for x in fx_symbol if x not in set(Upper_limit_Symbols)]
    fx_symbol.sort()
    split_symbol = split_list_n_parts(fx_symbol, n=5)

    if test:
        print(symbols)
        print(len(symbols))
        print(split_symbol)

    return_dict = {}  # The dict that is used for returning, thru JSON

    df_pivot_return = df_pivot.copy()
    df_pivot_return["SYMBOL"] = df_pivot_return["SYMBOL"].apply(lambda x: Symbol_spread_url_External(x))

    return_dict["H_y_scroll_1"] = df_pivot_return.to_dict("record")

    df_pivot_return = None # Free up some space.

    return_dict["P_Long_0"] = plot_symbol_Spread(df, "All Symbol Spread", x_axis=date_col)
    return_dict["P_Long_1"] = plot_symbol_Spread(df[df["SYMBOL"].str.startswith(".")], "All CFD Spread", x_axis=date_col)
    return_dict["P_Long_2"] =  plot_symbol_Spread(df[df["SYMBOL"].isin(Upper_limit_Symbols)], "High Spread Symbol", x_axis=date_col)


    # Will append to the list to be returned as figures/plots
    for i in range(len(split_symbol)):
        return_dict["P_Long_{}".format(i + 3)] = plot_symbol_Spread(df[df["SYMBOL"].isin(split_symbol[i])],
                                                                    "Symbol Spread [plot {}]".format(i+3), x_axis=date_col)


    #if test:
    print("Time taken for getting all symbol spread: {}".format((datetime.datetime.now() - time_start).total_seconds()))


    #print(return_dict)
    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps(return_dict, cls=plotly.utils.PlotlyJSONEncoder)



# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@Spread_bp.route('/Individual_Symbol_Spread/<symbol>', methods=['GET', 'POST'])
@roles_required()
def individual_symbol_spread(symbol):

    title = Markup("{} Spread".format(symbol))
    header = Markup("<b>Symbol Spread: {}</b>".format(symbol))

    description = Markup("""<b>Symbol Spread:  {}</b><br><br>
                Calculating the symbol spread using Live 1 q symbols.<br><br>
                <b>Mark Up</b><br>
                Mark up (If any) has been removed Based on SQ's current database. (no back tracking)<br><br>
                <b>Page Loading</b><br>
                Page will generally take about 20 seconds to load as there are quite a number of data points.<br><br>
                <b>Graph Controls</b><br>
                - <b>Click Once</b> on the Symbol in the graph legend to remove it.<br>
                - <b>Double Click</b> on the Symbol in the graph legend to isolate it.<br><br>
                <b>Date/Date Time</b><br>
                - Times are based on Live 1 server time.<br>
                - Date, for example, of 23th, means 22nd 2300 - 23 2259<br>
                - Time, for example, at 0600, means 0600 - 0659<br><br>
                Using Database: "sf_test" """.format(symbol))

    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("individual_symbol_spread"), icon="",
                           Table_name={ "Symbol Float": "H_y_scroll_1",
                                        "Plot Of all Spread" : "P_Long_0",
                                        "Plot Of Spread 1": "P_Long_1",
                                        },
                           title=title,
                           ajax_url=url_for('Spread.individual_symbol_spread_ajax', symbol=symbol.lower(),  _external=True),
                           header=header, ajax_timeout_sec = 250,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"])) #setinterval=60,






# The Ajax call for the symbols we want to query. B Book.
@Spread_bp.route('/individual_symbol_spread_ajax/<symbol>', methods=['GET', 'POST'])
@roles_required()
def individual_symbol_spread_ajax(symbol):

    test = False
    BGI_prefix = "BGI"
    # # Query the DB for the average ticks per day.
    time_start = datetime.datetime.now()

    # To get the number of days backwards.
    days_backwards = count_weekday_backwards(12) if not test else count_weekday_backwards(5)

    date = (datetime.datetime.now() - datetime.timedelta(days=days_backwards)).strftime("%Y-%m-%d")


    # --------------- Getting the symbol digits.
    symbol_digits_record = get_symbol_digits(symbol=symbol.lower())
    symbol_digits_df = pd.DataFrame(symbol_digits_record)
    symbol_digits = symbol_digits_df[symbol_digits_df["SYMBOL"] == symbol.upper()]["DIGITS"].values[0]
    if test:
        print(f"symbol_digits: {symbol_digits}")

    # Want to get other broker's ticks, by per hour
    Symbol_Ticks_Broker_unsync = get_other_broker_ticks_unsync(sql_database = "global_prime", symbol=symbol,
                                                               date_start=date, symbol_digits=symbol_digits,
                                                               app_unsync=current_app._get_current_object(),
                                                               pre_fix="GP")



    # Get it from the Ticks DB
    sql_query = "SELECT * from sf_test.live1q_hourly where DATE >= '{date}' AND SYMBOL RLIKE '{symbol}'".format( date=date, symbol=symbol)
    bgi_ticks_res = unsync_Query_SQL_ticks_db_engine(sql_query=sql_query,
                                           app_unsync=current_app._get_current_object(),
                                                       date_to_str=False)




    # ------------------------- Want to get the actual symbol markup.
    sql_query_markup = "SELECT SOURCESYMB, POSTFIXSYMB, BIDSPREAD, ASKSPREAD from live1.symbol_o where POSTFIXSYMB in ('{}q')".format(symbol)

    markup_res = Query_Symbol_Markup_db_engine(sql_query_markup)
    df_markup = pd.DataFrame(markup_res)

    # to remove the q postfix for each symbol. So that we can do a join
    df_markup["SYMBOL"] = df_markup["POSTFIXSYMB"].apply(lambda x: x.replace("q", ""))

    df = pd.DataFrame(bgi_ticks_res.result())
    # Change the name of the columns to reflect that it's BGI
    df.rename(columns={"AVG": "{}_AVG".format(BGI_prefix),
                       "MIN": "{}_MIN".format(BGI_prefix),
                       "MAX": "{}_MAX".format(BGI_prefix)}, inplace=True)

    #--------------- Merge the dfs so that we can get the correct markup.
    df = df.merge(df_markup, on="SYMBOL")
    # Calculate the markup
    df["TOTAL_MARKUP"] = df["ASKSPREAD"] - df["BIDSPREAD"]

    # Minus the markup for all the AVG, MIN and MAX
    list_of_col = [c for c in df.columns if any(c.find(x) >= 0 for x in ["MAX", "AVG", "MIN"])]
    for c in list_of_col:
        df[c] = df[c] - df["TOTAL_MARKUP"]


    # To Print it nicely.
    df["SYMBOL"] = df["SYMBOL"].str.upper()


    date_col = ""

    # To create timing for ALL hours of the day.
    # This includes date/times that might be have ticks,
    # To put the None timing in.
    df_time_all = create_all_timing(df)
    # For the hourly merge.

    # Check if the columns are in.
    if all([c in df for c in ["DATE", "HOUR"]]):
        # Append the hour to the date. Making it a datetime.
        df["DATE_TIME"] = df.apply(lambda x: x["DATE"] + pd.DateOffset(hours=x["HOUR"]), axis=1)
        date_col = "DATE_TIME"
        df = df.merge(df_time_all, how="outer", on="DATE_TIME").sort_values("DATE_TIME")
        #pd.set_option("max_rows", 500)

    elif "DATE" in df:
        # Print the dates correctly.
        date_col = "DATE"

    # Want to print the dates into string
    for c in df.columns:
        if c.lower().find("date") >=0 :
            df[c] = df[c].apply(lambda x: "{}".format(x))
            #df[c] = df[c].apply(lambda x: x.strftime("%Y-%m-%d"))



    df.sort_values(date_col, inplace=True)  # First, we need to sort the values

    return_dict = {}  # The dict that is used for returning, thru JSON


    # Plot the Figure. BGI Hourly Min/Average/Max
    return_dict["P_Long_0"] = plot_symbol_Spread_individual(df, "BGI {} Spread".format(symbol.upper()), x_axis=date_col)


    # Want to get ticks from another LP to compare.
    df_Symbol_Ticks_Broker = pd.DataFrame(Symbol_Ticks_Broker_unsync.result())
    df_Symbol_Ticks_Broker["DATE_TIME"] = df_Symbol_Ticks_Broker["DATE_TIME"].apply(lambda x: "{}".format(x))
    df = df.merge(df_Symbol_Ticks_Broker, how="outer", on="DATE_TIME").sort_values("DATE_TIME")

    if test:
        print(df)

    # We want the list of columns to plog
    list_of_col = [c for c in df.columns if any(c.find(x) >= 0 for x in ["MAX", "AVG", "MIN"])]

    print(list_of_col)

    # pd.concat([get_df_slice(df=df, col=c) for c in list_of_col])
    df_pivot = pd.pivot_table(pd.concat([get_df_slice(df=df, col=c) for c in list_of_col]),
                              values="VALUE", index=["TYPE"], columns=[date_col]).reset_index()

    df_pivot.fillna("-", inplace=True)
    df_pivot = np.round(df_pivot, 2)

    if test:
        print(df_pivot)

    return_dict["H_y_scroll_1"] = df_pivot.to_dict("record")






    # Plot the Figure. BGI Hourly Min/Average/Max
    return_dict["P_Long_1"] = plot_symbol_Spread_individual(df, "{} Spread Compare".format(symbol.upper()), x_axis=date_col)


    #if test:
    print("Time taken for getting Individual symbol spread: {}".format((datetime.datetime.now() - time_start).total_seconds()))

    #print(return_dict)
    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps(return_dict, cls=plotly.utils.PlotlyJSONEncoder)

