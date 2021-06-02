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

    title = Markup("<b>Symbol Spread</b>")
    header = Markup("<b>Symbol Spread</b>")

    description = Markup("""<b>Symbol Spread</b><br><br>Calculating the symbol spread using Live 2 q symbols. 
                Mark up (If any) has not been removed.<br><br>Page will generally take about 1 min to load.<br>
                CFDs not included for now.<br>""")




    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbol_float_trades"), icon="",
                           Table_name={ "Symbol Float": "H1",
                                        "Plot Of all Spread" : "P_Long_0",
                                        "Plot Of Spread 1": "P_Long_1",
                                        "Plot Of Spread 2": "P_Long_2",
                                        "Plot Of Spread 3": "P_Long_3",
                                        "Plot Of Spread 4": "P_Long_4",
                                        "Plot Of Spread 5": "P_Long_5",
                                        },
                           title=title,
                           ajax_url=url_for('Spread.symbol_spread_ajax', _external=True),
                           header=header, ajax_timeout_sec = 250,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"])) #setinterval=60,




# The Ajax call for the symbols we want to query. B Book.
@Spread_bp.route('/symbol_spread_ajax', methods=['GET', 'POST'])
@roles_required()
def symbol_spread_ajax():

    test = False
    days_backwards = count_weekday_backwards(12) if not test else count_weekday_backwards(5)

    # Want to get all the tables that are in the DB
    sql_query = """SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = "BGI_Live2" """

    # Get it from the Ticks DB
    res = Query_SQL_ticks_db_engine(sql_query)

    # Find the tables postfixed with "ticks"
    sql_tables = [r['TABLE_NAME'] for r in res if r['TABLE_NAME'].find("ticks") > 0]

    # Get all the symbols that we need.
    symbols = list(set([s[0:6] for s in sql_tables if s.find(".") == -1]))
    symbols.sort()


    # The actual symbol name.
    to_query = []
    for s in symbols:
        if s + "q_ticks" in sql_tables:
            to_query.append((s, s + "q_ticks"))
        if test:    # If we are testing, we want to stop at 5 to save sometime.
            if len(to_query) >= 5:
                break
        # else:
        #     print(s)


    sql_average = """SELECT DATE_ADD(DATE_TIME, INTERVAL 1 HOUR) as DATE_TIME, '{symbol}' as Symbol, AVG(ASK-BID) as SPREAD
    FROM BGI_Live2.{table_name} 
    WHERE DATE_TIME > "{date} 23:00:00"
    GROUP BY LEFT(DATE_ADD(DATE_TIME, INTERVAL 1 HOUR) , 10)"""

    date = (datetime.datetime.now() - datetime.timedelta(days=days_backwards)).strftime("%Y-%m-%d")
    all_query_list = [sql_average.format(symbol=s, table_name=t, date=date) for (s, t) in to_query]


    # Query the DB for the average ticks per day.
    time_start = datetime.datetime.now()

    # Want to split up the SQL calls to reduce time taken
    to_split_num = 5 # How many to split it into
    # unsync_results = [] # To save all the unsync Results
    #
    # for i in range(to_split_num):
    #
    #     start_index = i * math.floor(len(all_query_list) / to_split_num)
    #     end_index = min( (i + 1) * math.floor(len(all_query_list) / to_split_num), len(all_query_list) -1)
    #     if test:
    #         print("Getting SQL for list of [{} : {}]".format(start_index, end_index))
    #
    #     sql_query = " UNION ".join(all_query_list[start_index: end_index]).replace("\n", " ")
    #     unsync_results.append(unsync_Query_SQL_ticks_db_engine(sql_query=sql_query,
    #                                                 app_unsync = current_app._get_current_object(),
    #                                                 date_to_str = False))


    # First, split the Query into n parts.
    query_split_list = split_list_n_parts(all_query_list, n=to_split_num)
    # UNION them all together. So now we have n queries.
    sql_query_list = [" UNION ".join(q).replace("\n", " ") for q in query_split_list]
    # Does the SQL calls in an unsync manner
    unsync_results = [unsync_Query_SQL_ticks_db_engine(sql_query=s,
                                               app_unsync=current_app._get_current_object(),
                                                   date_to_str=False) for s in sql_query_list]
    # Wait to get it all back, to sync it.
    res_list = [pd.DataFrame(r.result()) for r in unsync_results]


    #if test:
    print("Time taken for getting all symbol spread: {}".format((datetime.datetime.now() - time_start).total_seconds()))


    #col = [c[0] for c in res[1]]
    df = pd.concat(res_list)
    df["DATE_TIME"] = df["DATE_TIME"].apply(lambda x: x.strftime("%Y-%m-%d"))

    if test:
        print(df)

    sql_digit_query = """SELECT Symbol, Digits FROM LIVE2.MT4_SYMBOLS WHERE SYMBOL IN ({})""".format(",".join([f"'{s}'" for s in symbols]))

    digits_data = Query_SQL_db_engine(sql_digit_query)

    df_digits = pd.DataFrame(digits_data)



    df["Symbol"] = df["Symbol"].str.upper()

    df = df.merge(df_digits, on="Symbol")
    df["Spread_Digits"] = df["SPREAD"] * 10 ** df["Digits"]

    if test:
        print(df_digits)

    df_pivot = pd.pivot_table(df, values="Spread_Digits", index=["Symbol"], columns=["DATE_TIME"]).reset_index()

    if test:
        print(df_pivot)

    #df_pivot

    df_pivot = np.round(df_pivot, 2)

    df.sort_values(["DATE_TIME", "Symbol"], inplace=True) # First, we need to sort the values

    all_symbol = df["Symbol"].unique().tolist()
    all_symbol.sort()
    split_symbol = split_list_n_parts(all_symbol, n=5)

    if test:
        print(all_symbol)
        print(len(all_symbol))
        print(split_symbol)

    return_dict = {}     # The dict that is used for returning, thru JSON
    return_dict["H1"] = df_pivot.to_dict("record")
    return_dict["P_Long_0"] =  plot_symbol_Spread(df, "All Symbol Spread")

    # Will append to the list to be returned as figures/plots
    for i in range(len(split_symbol)):
        return_dict["P_Long_{}".format(i+1)] = plot_symbol_Spread(df[df["Symbol"].isin(split_symbol[i])], "Symbol Spread {}".format(split_symbol[i]))


    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps(return_dict, cls=plotly.utils.PlotlyJSONEncoder)

