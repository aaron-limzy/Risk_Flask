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

    title = "Symbol Spread"
    header = "Symbol Spread"


    table_ledgend = "Symbol Spread"

    description = Markup("Symbol Spread")



    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbol_float_trades"), icon="",
                           Table_name={ "Symbol Float": "H1",
                                        "Plot Of Spread" : "P_Long_1",
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

    sql_query = " UNION ".join(all_query_list).replace("\n", " ")

    # Query the DB for the average ticks per day.
    time_start = datetime.datetime.now()
    res = Query_SQL_ticks_db_engine(sql_query)

    if test:
        pass

    print("Time taken: {}".format((datetime.datetime.now() - time_start).total_seconds()))



    #col = [c[0] for c in res[1]]
    df = pd.DataFrame(res)
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
    fig = px.line(df, x="DATE_TIME", y="Spread_Digits", color='Symbol')

    # Figure Layout.
    fig.update_layout(
        autosize=True,
        yaxis=dict(
            title_text="Spread (Points)",
            titlefont=dict(size=20),
            automargin=True,
            ticks="outside", tickcolor='white', ticklen=50,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Date",
            titlefont=dict(size=20),
            automargin=True,
            layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text='Symbol Spread',
        titlefont=dict(size=20),
        title_x=0.5,
        margin=dict(
            pad=10)
    )

    fig.update_yaxes(automargin=True)

    #fig.show()




    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps({"H1": df_pivot.to_dict("record"),
                       "P_Long_1": fig}, cls=plotly.utils.PlotlyJSONEncoder)

