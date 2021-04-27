

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
#
# import pandas as pd
# import numpy as np
# import json

from app.decorators import roles_required
from app.Plotly.forms import Live_Login
from app.Plotly.tableau_url import *
# from app.Plotly.table import Client_Trade_Table

from app.background import *
# #from app.tableau_embed import *
#
# from app.Risk_Tools_Config import email_flag_fun
#
# import emoji
# import flag
#
# from app.Swaps.forms import UploadForm
#
# from flask_table import create_table, Col
#
# import requests
#
# import pyexcel
# from werkzeug.utils import secure_filename

from Helper_Flask_Lib import *
from app.Plotly.Client_Trade_Analysis import *

import decimal

mt5_monitoring = Blueprint('mt5_monitoring', __name__)


# For MT5 Monitoring and saving tools.


@mt5_monitoring.route('/Save_BGI_mt5_Float', methods=['GET', 'POST'])
@roles_required()
def save_mt5_BGI_float():
    title = "Save MT5 BGI Float"
    header = "Saving MT5 BGI Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    description = Markup("<b>Saving MT5 BGI Float Data.</b><br>" +
                         "Saving it into (149.213) aaron.bgi_mt5_float_save<br>" +
                         "Save it by Country, by Symbol.<br>" +
                         "Country Float and Symbol Float will be using this page.<br>" +
                         "Table time is in Server Timing.<br>")

    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename=background_pic("save_mt5_BGI_float"),
                           icon="css/save_Filled.png", Table_name="Save MT5 Floating 💾", title=title,
                           ajax_url=url_for('mt5_monitoring.save_BGI_MT5_float_Ajax', _external=True), header=header,
                           setinterval=15,
                           description=description, replace_words=Markup(["Today"]))


# Insert into aaron.BGI_Float_History_Save
# Will select, pull it out into Python, before inserting it into the table.
# Tried doing Insert.. Select. But there was desdlock situation..
@mt5_monitoring.route('/save_BGI_mt5_float_Ajax', methods=['GET', 'POST'])
@roles_required()
def save_BGI_MT5_float_Ajax(update_tool_time=1):
    # start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([{'Update time': "Not updating, as Market isn't opened. {}".format(Get_time_String())}])
    #
    # if check_session_live1_timing() == False:  # If False, It needs an update.
    #
    #     # TOREMOVE: Comment out the print.
    #     print("Saving Previous Day PnL")
    #     # TODO: Maybe make this async?
    #     #save_previous_day_PnL()  # We will take this chance to get the Previous day's PnL as well.

    # Want to reduce the query overheads. So try to use the saved value as much as possible.
    server_time_diff_str = session["live1_sgt_time_diff"] - 1 if "live1_sgt_time_diff" in session \
        else '(SELECT result FROM aaron.`aaron_misc_data` where item = "mt5_timing_diff")'

    query = mt5_BBook_select_insert(time_diff=server_time_diff_str)

    result_data = SQL_insert_MT5_statement(query)

    # Insert into the Checking tools.
    if (update_tool_time == 1):
        Tool = "bgi_float_mt5_history_save"
        sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
                     " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
                         Tool=Tool)
        raw_insert_result = db.engine.execute(sql_insert)

    # end = datetime.datetime.now()
    # print("\nSaving floating PnL tool: {}s\n".format((end - start).total_seconds()))
    return json.dumps([{'Update time': Get_time_String()}])






@mt5_monitoring.route('/Futures_LP_Details', methods=['GET', 'POST'])
@roles_required()
def Futures_LP_Details():
    title = "Futures_LP_Details"
    header = "Futures_LP_Details"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    description = Markup("Details are on BGI Side.<br>There are various accounts, each with a different wallet for each currency.")



    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename=background_pic("Futures_LP_Details"),
                           icon="css/save_Filled.png", Table_name="Futures LP Details", title=title,
                           ajax_url=url_for('mt5_monitoring.Futures_LP_Details_Ajax', _external=True), header=header,
                           setinterval=15, no_backgroud_Cover=True,
                           description=description, replace_words=Markup(["Today"]))





# Insert into aaron.BGI_Float_History_Save
# Will select, pull it out into Python, before inserting it into the table.
# Tried doing Insert.. Select. But there was desdlock situation..
@mt5_monitoring.route('/mt5_futures_LP_data_Ajax', methods=['GET', 'POST'])
@roles_required()
def Futures_LP_Details_Ajax(update_tool_time=1):

    data = mt5_futures_LP_data()
    df = pd.DataFrame(data)
    #print(df)
    if 'DATETIME' in df:
        df['DATETIME'] =  df['DATETIME'].apply(lambda x : "{}".format(x))

    df["PnL"] = df['EQUITY'] -  df['BALANCE']

    cols = ['EQUITY', 'CANDRAW', "ACCTINITIALMARGIN", "BALANCE", "ACCTMAINTENANCEMARGIN", "FROZENFEE", "MARKETEQUITY", "PnL"]
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
    # end = datetime.datetime.now()
    # print("\nSaving floating PnL tool: {}s\n".format((end - start).total_seconds()))
    return json.dumps(df[cols_to_display].to_dict('record'))



@mt5_monitoring.route('/BGI_MT5_Symbol_Float', methods=['GET', 'POST'])
@roles_required()
def BGI_MT5_Symbol_Float():
    title = "MT5 Symbol Float"
    header = "MT5 Symbol Float"

    description = Markup("<b>Floating PnL By Symbol for MT5.</b><br>" + \
                         "Values are on BGI Side.<br> If it's a monday, 'Yesterday' refers to the sum of Friday + Sat + Sunday, where applicable'.")

    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename=background_pic("BGI_MT5_Symbol_Float"),
                           icon="",
                           Table_name="MT5 Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('mt5_monitoring.BGI_MT5_Symbol_Float_ajax', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(Client Side)']))


# Get BGI Float by Symbol
@mt5_monitoring.route('/BGI_MT5_Symbol_Float_ajax', methods=['GET', 'POST'])
@roles_required()
def BGI_MT5_Symbol_Float_ajax():
    # start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time': "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    df_to_table = mt5_symbol_float_data()

    #print(df_to_table)

    if len(df_to_table) == 0:
        return json.dumps([[{"Comment": "No Floating position for MT5"}], "{}".format(datetime.datetime.now()), "-"],
                          cls=plotly.utils.PlotlyJSONEncoder)

    yesterday_datetime_pull = ["'Yesterday Datetime' not available."]

    if "YESTERDAY_DATETIME_PULL" in df_to_table:
        # Get Datetime into string
        df_to_table['YESTERDAY_DATETIME_PULL'] = df_to_table['YESTERDAY_DATETIME_PULL'].apply(lambda x: Get_time_String(x) if isinstance(x, pd.Timestamp) else x)
        #print( df_to_table['YESTERDAY_DATETIME_PULL'] )
        yesterday_datetime_pull = [c for c in list(df_to_table['YESTERDAY_DATETIME_PULL'][df_to_table['YESTERDAY_DATETIME_PULL'].notnull()].unique()) if c != 0]
        #print(yesterday_datetime_pull)


    datetime_pull=["No Datetime in df."]
    if 'DATETIME' in df_to_table:
        df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x) if isinstance(x, pd.Timestamp) else x)
        datetime_pull = [c for c in list(df_to_table['DATETIME'][df_to_table['DATETIME'].notnull()].unique()) if c != 0]


    # Sort by abs net volume
    if "ABS_NET" in df_to_table:
        df_to_table["ABS_NET"] = df_to_table["NET_LOTS"].apply(lambda x: abs(x))
        df_to_table.sort_values(by=["ABS_NET"], inplace=True, ascending=False)
        df_to_table.pop('ABS_NET')

    # We already know the date. No need to carry on with this data.
    if "DATETIME" in df_to_table:
        df_to_table.pop('DATETIME')

    # # # Want to add colors to the words.
    # Want to color the REVENUE

    cols_to_str = ["REVENUE", "TODAY_REVENUE", "NET_LOTS", "YESTERDAY_LOTS", "YESTERDAY_REVENUE", "YESTERDAY_REBATE"]

    for c in cols_to_str:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].fillna("-") # Fill it with "-" if it's null.
            df_to_table[c] = df_to_table[c].apply(
                lambda x: """{c}""".format(c=profit_red_green(x)) if isfloat(x) else x)


    # Want only those columns that are in the df
    # Might be missing cause the PnL could still be saving.
    # If so, we force it to be "-"
    #print(df_to_table[["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE"]])

    col = ["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE", "TODAY_LOTS", "TODAY_REVENUE", \
           "YESTERDAY_LOTS", "YESTERDAY_REVENUE", "YESTERDAY_REBATE"]
    for c in col:
        if c not in df_to_table:
            df_to_table[c] = "-"

    # Want to fill up all the NA with "-"
    df_to_table.fillna("-", inplace=True)

    # Pandas return list of dicts.
    return_val = df_to_table[col].to_dict("record")

    return json.dumps([return_val, ", ".join(datetime_pull), ", ".join(yesterday_datetime_pull)], cls=plotly.utils.PlotlyJSONEncoder)


