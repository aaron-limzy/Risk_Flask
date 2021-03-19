from flask import Blueprint, render_template, Markup, url_for, request, session, current_app, flash, redirect
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from app.Swaps.A_Utils import *
from sqlalchemy import text
from app.extensions import db, excel

from app.mt5_queries.mt5_sql_queries import *
from app.mt5_queries.mt5_helper_functions import *

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

mt4_mt5_bp = Blueprint('mt4_mt5', __name__)


@mt4_mt5_bp.route('/BGI_MT5_Symbol_Float', methods=['GET', 'POST'])
@roles_required()
def BGI_MT5_Symbol_Float():

    title = "MT5 Symbol Float"
    header = "MT5 Symbol Float"


    description = Markup("<b>Floating PnL By Symbol for MT5.</b><br>")




        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename=background_pic("BGI_Symbol_Float"),
                           icon= "",
                           Table_name="MT5 Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('mt4_mt5.BGI_MT5_Symbol_Float_ajax', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(Client Side)']))


# Get BGI Float by Symbol
@mt4_mt5_bp.route('/BGI_MT5_Symbol_Float_ajax', methods=['GET', 'POST'])
@roles_required()
def BGI_MT5_Symbol_Float_ajax():


    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])


    #print(df_yesterday_symbol_pnl)

    server_time_diff_str = session["live1_sgt_time_diff"] -1 if "live1_sgt_time_diff" in session \
                else '(SELECT result FROM aaron.`aaron_misc_data` where item = "mt5_timing_diff")'

    sql_query = """SELECT SYMBOL, ROUND(SUM(NET_FLOATING_VOLUME),2) as `NET_LOTS`, ROUND(SUM(FLOATING_VOLUME),2) as `FLOATING_LOT`, ROUND(SUM(FLOATING_REVENUE),2) as `REVENUE`, 
        ROUND(SUM(CLOSED_VOL_TODAY),2) as `TODAY_LOTS`,
        ROUND(SUM(CLOSED_REVENUE_TODAY),2) as `TODAY_REVENUE`,  DATETIME
            FROM  aaron.bgi_mt5_float_save
            WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.bgi_mt5_float_save)
                GROUP BY SYMBOL
            ORDER BY floating_revenue DESC
        """
    result_data = Query_SQL_mt5_db_engine(sql_query)


    if len(result_data) == 0:
        return json.dumps([[{"Comment": "No Floating position for MT5"}], "{}".format(datetime.datetime.now()), "-"],
                          cls=plotly.utils.PlotlyJSONEncoder)


    #end = datetime.datetime.now()
    #print("\nGetting SYMBOL PnL tool[After Query]: {}s\n".format((end - start).total_seconds()))

    df = pd.DataFrame(result_data)

    # For the display of table, we only want the latest data inout.
    df_to_table = df[df['DATETIME'] == df['DATETIME'].max()].drop_duplicates()

    # Get Datetime into string
    df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x))


    datetime_pull =  [c for c in list(df_to_table['DATETIME'].unique()) if c != 0] if  "DATETIME" in df_to_table else  ["No Datetime in df."]



    # Sort by abs net volume
    df_to_table["ABS_NET"] = df_to_table["NET_LOTS"].apply(lambda x: abs(x))
    df_to_table.sort_values(by=["ABS_NET"], inplace=True, ascending=False)
    df_to_table.pop('ABS_NET')


    # We already know the date. No need to carry on with this data.
    if "DATETIME" in df_to_table:
        df_to_table.pop('DATETIME')





   # # # Want to add colors to the words.
    # Want to color the REVENUE
    cols = ["REVENUE", "TODAY_REVENUE", "NETVOL"]
    for c in cols:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: """{c}""".format(c=profit_red_green(x) if isfloat(x) else x))



    # #Rename the VOLUME to LOTs
    # df_to_table.rename(columns={"NETVOL": "NET_LOTS", "VOLUME": "FLOATING_LOTS",
    #                             "TODAY_VOL" : "TODAY_LOTS", "YESTERDAY_VOLUME": "YESTERDAY_LOTS"}, inplace=True)


    # Want only those columns that are in the df
    # Might be missing cause the PnL could still be saving.
    col_of_df_return = [c for c in ["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE", "TODAY_LOTS", \
                                    "TODAY_REVENUE"] \
                        if c in  list(df_to_table.columns)]

    # Pandas return list of dicts.
    return_val = df_to_table[col_of_df_return].to_dict("record")



    return json.dumps([return_val, ", ".join(datetime_pull), " - "], cls=plotly.utils.PlotlyJSONEncoder)
