import pandas as pd
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
    header = "Saving MT5 BGI Floating data"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    description = Markup("<b>Saving MT5 BGI Float Data.</b><br>" +
                         "Saving it into (149.213) aaron.bgi_mt5_float_save<br>" +
                         "Save it by Country, by Symbol.<br>" +
                         "Country Float and Symbol Float will be using this page.<br>" +
                         "Table time is in Server Timing.⏱<br>")



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
@roles_required(["Dealing", "Risk", "Admin", "Risk_TW"])
def Futures_LP_Details():
    title = "Futures_LP_Details"
    header = "Futures_LP_Details"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    description = Markup("Details are on BGI Side.<br>There are various accounts, each with a different wallet for each currency.<br>" + \
                         "<br><b><u>SQL Details.</u></b><br>Uses SQL IP: 119.81.149.213. <br>Table: e800941.`esunny_account` , e91220008.`esunny_account`<br>" +\
                         "SQL Procedure. call aaron.Futures_LP_Details()<br>" + \
                         "<br><b>There are currently no alerts set on these details.</b><br>" +\
                         "<br>SQ/Vic would be keeping this table updated. ")



    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename=background_pic("Futures_LP_Details"),
                           icon=icon_pic("Futures_LP_Details") ,Table_name="Futures LP Details", title=title,
                           ajax_url=url_for('mt5_monitoring.Futures_LP_Details_Ajax', _external=True), header=header,
                           setinterval=15, no_backgroud_Cover=True,
                           description=description, replace_words=Markup(["Today"]))





@mt5_monitoring.route('/mt5_futures_LP_data_Ajax', methods=['GET', 'POST'])
@roles_required(["Dealing", "Risk", "Admin", "Risk_TW"])
def Futures_LP_Details_Ajax(update_tool_time=1):

    data = mt5_futures_LP_data()

    # Use MT5 Helper function to make the DF presentable. Looks nicer in the HTML
    df = pretty_print_mt5_futures_LP_details(pd.DataFrame(data))

    # end = datetime.datetime.now()
    # print("\nSaving floating PnL tool: {}s\n".format((end - start).total_seconds()))
    return json.dumps(df.to_dict('record'))





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

    # pd.set_option("display.max_rows", 101)
    # pd.set_option("display.max_columns", 101)

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
        yesterday_datetime_pull = ["{}".format(c) for c in list(df_to_table['YESTERDAY_DATETIME_PULL'][df_to_table['YESTERDAY_DATETIME_PULL'].notnull()].unique()) if c != 0]
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
    return_val = df_to_table[col].to_dict("records")

    # print(df_to_table[col])
    # print(datetime_pull)
    # print(yesterday_datetime_pull)

    return json.dumps([return_val, ", ".join(datetime_pull), ", ".join(yesterday_datetime_pull)], cls=plotly.utils.PlotlyJSONEncoder)


# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@mt5_monitoring.route('/HK_Copy_STP', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Dealing"])
def HK_Copy_STP():


    header = Markup("<b><u>HK A Book</u></b>")
    title =  "HK A Book"

    description = Markup("""
        <h3>Table Description</h3>
        <span style="color:green">Trade Copier EA</span><br>
        The lots being copied (STP) will be rounded up to the nearest <span style="color:red">2 decimal places</span>. For example,<br>
        <span style="color:red"><u>25% of 1.5 lots  =0.375 lot, the trade will be hit out as 0.38 lots</u></span>.
        <br><br>
        <span style="color:green">[Total Closed Pnl Table]</span><br>
        -Shows the total closed lots and profit of 800243 and its corresponding LPs since its inception.<br>
        -The <span style="color:blue">Profit</span> value in 800243 is in BGI perspective.<br>
        -<span style="color:blue">"Blackwell Global"</span> LP consist trades that we send out to our LPs, including GlobalPrime,Vantage and CFH. <br>
        *Our current LP is CFH. 
        <br><br>
        
        <span style="color:green">[Floating Lots Table]</span><br>
        -<span style="color:blue">Net Lots * STP% </span> is calculate on each individual trades hit out to LPs as each trades will be rounded off to the nearest 2 decimal places.<br>
        -Therefore <span style="color:blue">LP Net Lots - Net Lots * STP% </span> row is used for error checking. <span style="color:red"><b>The value should always be 0, otherwise an alert will be sent to notify the mismatch in Lots.</b></span>
        
        <br><br>
        
        <span style="color:green">[Open Positions Table]</span><br> 
        -Shows a quick comparison between the LPs' volume/profit/price/open time 
        <br><br>
        <span style="color:green">[LP Details Table]</span><br>
        -Shows our current funding in our LPs.<br>
        <br><br>
        <span style="color:green">[Close Trade Table]</span><br>
        -Shows close trade comparison between LP's price/profit/lot/open time/close time in the past 24 hours
        <br>
        <br><b>SQL INFO</b><br>
        <br>
        The account being STP,STP % and CFH trade copier's login can be set in sql table [aaron.aaron_misc_data] <br>
        <ul>
        <li>item=HK_Copy_Trade_Main_Login </li>
        <li>item = HK_Copy_Trade_CFH_Login</li>
        <li>item = HK_Copy_Trade_Percentage</li>
        <li>item = HK_Copy_Trade_Percentage_CFH</li>
        <li>item = HK_Copy_Trade_Percentage_BIC</li>
        <li>item = HK_Copy_Trade_Percentage_SQ</li>
        </ul>
        <br>
        The trades information can be taken from 
        <ul>
        <li>Vantage/CFH -> MT4 live 1 </li>
        <li>BIC -> aaron.live_trade_88803164_b.i.c.markets_live </li>
        <li>SwissQuote -> aaron.live_trade_714009_swissquote_live </li>
        <li>Philips -> MT5 positions/deals </li>
        </ul>
        <br>
        Closed trades information can be taken procedures under aaron db (MT4/MT5)from
        <ul>
        <li>HK_CopyTrade_Open_Time_Comparison_last24hour </li>
        <li>HK_CopyTrade_Price_Comparison_last24hour</li>
        <li>HK_CopyTrade_Profit_Comparison_last24hour </li>
        </ul>                                               
            """)


    return render_template("Wbwrk_Multitable_Borderless_redalert.html", backgroud_Filename=background_pic("HK_Copy_STP"), icon=icon_pic("ABook_BGI"),
                           Table_name={"TOTAL CLOSED PNL": "Hss1",
                                       "FLOATING LOTS": "Vss1",
                                       "FLOATING PROFIT": "Vss2",
                                       "[OPEN] PRICE & PROFIT COMPARISON": "H1",
                                       "[OPEN] Lots & Open Time Comparison": "H2",
                                       "LP Details": "H3",
                                       "Line1"                           : "Hr1" ,
                                       "[CLOSED] LOT/PROFIT COMPARISON": "H4",
                                       "[CLOSED] OPEN/CLOSE PRICE COMPARISON": "H5",
                                       "[CLOSED] OPEN/CLOSE TIME COMPARISON"         : "H6"
                                       },
                           title=title, setinterval=30,
                           ajax_url=url_for('mt5_monitoring.HK_Copy_STP_ajax', _external=True),
                           header=header,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["mismatch", "so alert:", "alert"])) #setinterval=60,



@mt5_monitoring.route('/HK_Copy_STP_ajax', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Dealing"])
def HK_Copy_STP_ajax(update_tool_time=0):    # To upload the Files, or post which trades to delete on MT5

    #start_time = datetime.datetime.now()

    #print("1")
    # First table
    # MT4 + MT5 Copy Trades Total PnL
    mt4_HK_CopyTrade_Total_Pnl_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Total_Pnl()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Total_Pnl_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Total_Pnl()", unsync_app=current_app._get_current_object())

    #print("2")
    # Second table
    #[HK_CopyTrade_NetLots_Difference]+MT5[HK_CopyTrade_NetLots_Difference]
    mt4_HK_CopyTrade_NetLots_Difference_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_NetLots_Difference()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_NetLots_Difference_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_NetLots_Difference()", unsync_app=current_app._get_current_object())

    #print("3")
    # Third table
    mt4_HK_CopyTrade_Profit_Difference_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Profit_Difference()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Profit_Difference_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Profit_Difference()", unsync_app=current_app._get_current_object())

    #print("4")
    # 4th table
    mt4_HK_CopyTrade_Price_Comparison_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Price_Comparison_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison()", unsync_app=current_app._get_current_object())

    #print("5")
    # 5th Table
    mt4_HK_CopyTrade_Open_Time_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Open_Time_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison()", unsync_app=current_app._get_current_object())

    #print("7")
    # 7th Table
    #[HK_CopyTrade_Profit_Comparison_last24hour] + MT5.[HK_CopyTrade_Profit_Comparison_last24hour]

    mt4_HK_CopyTrade_Profit_Comparison_last24hour_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Profit_Comparison_last24hour()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Profit_Comparison_last24hour_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Profit_Comparison_last24hour()", unsync_app=current_app._get_current_object())

    #print("8")
    # 8th Table
    # [HK_CopyTrade_Price_Comparison_last24hour] + MT5.[HK_CopyTrade_Price_Comparison_last24hour]
    mt4_HK_CopyTrade_Price_Comparison_last24hour_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison_last24hour()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Price_Comparison_last24hour_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison_last24hour()", unsync_app=current_app._get_current_object())

    #print("9")
    # 9th Table
    #[HK_CopyTrade_Open_Time_Comparison] + MT5.[HK_CopyTrade_Open_Time_Comparison]
    mt4_HK_CopyTrade_Open_Time_Comparison_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison_last24hour()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Open_Time_Comparison_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison_last24hour()", unsync_app=current_app._get_current_object())

    #print("6")
    # 6th Table
    # # While waiting, we will call somthing that isn't unsync
    lp_details = ABook_LP_Details_function(exclude_list=["Vantage", "GlobalPrime", "demo"])
    mt5_hk_LP_Copy_futures_data_unsync = mt5_HK_CopyTrade_Futures_LP_data(unsync_app=current_app._get_current_object())


    # ----------- To get the results ------

    #print("1")
    mt4_HK_CopyTrade_Total_Pnl = mt4_HK_CopyTrade_Total_Pnl_unsync.result()
    mt4_HK_CopyTrade_Total_Pnl_df = color_profit_for_df(mt4_HK_CopyTrade_Total_Pnl, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Total_Pnl = mt5_HK_CopyTrade_Total_Pnl_unsync.result()
    mt5_HK_CopyTrade_Total_Pnl_df = color_profit_for_df(mt5_HK_CopyTrade_Total_Pnl, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_1_concat = pd.concat([mt4_HK_CopyTrade_Total_Pnl_df, mt5_HK_CopyTrade_Total_Pnl_df])
    table_1_concat = table_1_concat if len(table_1_concat) > 0 else [{"Run Results": "No Open Trades"}]
    table_1_concat_return_data =  table_1_concat.to_dict("record")

    mt4_HK_CopyTrade_Total_Pnl = None
    mt5_HK_CopyTrade_Total_Pnl = None
    mt4_HK_CopyTrade_Total_Pnl_df = None
    mt5_HK_CopyTrade_Total_Pnl_df = None

    #print("2")
    # Second table ---
    mt4_HK_CopyTrade_NetLots_Difference = mt4_HK_CopyTrade_NetLots_Difference_unsync.result()
    mt4_HK_CopyTrade_NetLots_Difference_df = color_profit_for_df(mt4_HK_CopyTrade_NetLots_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_NetLots_Difference = mt5_HK_CopyTrade_NetLots_Difference_unsync.result()
    mt5_HK_CopyTrade_NetLots_Difference_df = color_profit_for_df(mt5_HK_CopyTrade_NetLots_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_2_return_data =  {}

    if all([c in mt4_HK_CopyTrade_NetLots_Difference_df for c in
            ["Net Lots", "LP Net Lots", "STP%", "LP Net Lots - Net Lots * STP%"]]) and "Philip Net Lots" in mt5_HK_CopyTrade_NetLots_Difference_df:
        table_2_return_data["Net Lots"] = mt4_HK_CopyTrade_NetLots_Difference_df["Net Lots"].sum()
        table_2_return_data["LP Net Lots"] = mt4_HK_CopyTrade_NetLots_Difference_df["LP Net Lots"].sum() + mt5_HK_CopyTrade_NetLots_Difference_df["Philip Net Lots"].sum()
        table_2_return_data["STP%"] = mt4_HK_CopyTrade_NetLots_Difference_df["STP%"].sum()
        table_2_return_data["LP Net Lots - Net Lots * STP%"] = mt4_HK_CopyTrade_NetLots_Difference_df["LP Net Lots - Net Lots * STP%"].sum() + \
                            mt5_HK_CopyTrade_NetLots_Difference_df["Philip Net Lots"].sum()
    else:
        table_2_return_data = {"Run Results": "No Open Trades"}

    #print("3")
    # Third table
    mt4_HK_CopyTrade_Profit_Difference = mt4_HK_CopyTrade_Profit_Difference_unsync.result()
    mt4_HK_CopyTrade_Profit_Difference_df = color_profit_for_df(mt4_HK_CopyTrade_Profit_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    mt5_HK_CopyTrade_Profit_Difference = mt5_HK_CopyTrade_Profit_Difference_unsync.result()
    mt5_HK_CopyTrade_Profit_Difference_df = color_profit_for_df(mt5_HK_CopyTrade_Profit_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    # print(mt5_HK_CopyTrade_Profit_Difference_df)
    # print(mt5_HK_CopyTrade_Profit_Difference_df)
    #print("mt4_HK_CopyTrade_Profit_Difference_df")
    #print(mt4_HK_CopyTrade_Profit_Difference_df)

    table_3_return_data = {}
    if all([c in mt4_HK_CopyTrade_Profit_Difference_df for c in ["LP Profit", "Profit"]]) and \
            all([c in mt5_HK_CopyTrade_Profit_Difference_df for c in ["Philip Profit" ]]):
        #table_3_return_data = {}
        table_3_return_data["LP Profit"] = mt4_HK_CopyTrade_Profit_Difference_df['LP Profit'].sum() + mt5_HK_CopyTrade_Profit_Difference_df["Philip Profit"].sum()
        table_3_return_data["Profit USD"] = mt4_HK_CopyTrade_Profit_Difference_df["Profit"].sum()
        table_3_return_data["Net Profit"] = table_3_return_data["LP Profit"] - table_3_return_data["Profit USD"]
        # To color the profit columns
        table_3_return_data = color_profit_for_df([table_3_return_data], default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=False)
    else:
        table_3_return_data = [{"Run Results": "No Open Trades"}]

    #print("4")
    # 4th Table
    mt4_HK_CopyTrade_Price_Comparison = mt4_HK_CopyTrade_Price_Comparison_unsync.result()
    mt4_HK_CopyTrade_Price_Comparison_df = color_profit_for_df(mt4_HK_CopyTrade_Price_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    mt5_HK_CopyTrade_Price_Comparison = mt5_HK_CopyTrade_Price_Comparison_unsync.result()
    mt5_HK_CopyTrade_Price_Comparison_df = color_profit_for_df(mt5_HK_CopyTrade_Price_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    if "Merging Ticket" in mt4_HK_CopyTrade_Price_Comparison_df and "Merging Ticket" in mt5_HK_CopyTrade_Price_Comparison_df:
        table_4_df = mt4_HK_CopyTrade_Price_Comparison_df.merge(mt5_HK_CopyTrade_Price_Comparison_df, how="left", left_on="Merging Ticket", right_on = "Merging Ticket")

        # Create Extra Column, considering to check if the columns are around.
        if all([c for c in ['Profit', 'CFH Profit', 'BIC Profit', 'SQ Profit', 'Philip Profit'] if
                c in table_4_df]):
            table_4_df["BGI Profit"] = table_4_df['CFH Profit'] + table_4_df['BIC Profit'] + table_4_df[
                'SQ Profit'] + table_4_df['Philip Profit'] - table_4_df['Profit']
        table_4_col = ['Ticket', 'Symbol', 'Open Price', 'CFH Open Price', 'BIC Open Price',
                       'SQ Open Price', 'Philip Open Price', 'Profit', 'CFH Profit',
                       'BIC Profit', 'SQ Profit', 'Philip Profit', "BGI Profit"]

        # To color profit columns
        table_4_df = color_profit_for_df(table_4_df.to_dict("record"), default=[{"Run Results": "No Open Trades"}],
                                         words_to_find=["profit"], return_df=True)

        table_4_df = table_4_df[[c for c in table_4_col if c in table_4_df]]
        table_4_df.fillna("-", inplace=True)

    else:
        table_4_df = pd.DataFrame([{"Run Results": "No Open Trades"}])

    #print("5")
    # 5th Table

    mt4_HK_CopyTrade_Open_Time = mt4_HK_CopyTrade_Open_Time_unsync.result()
    mt4_HK_CopyTrade_Open_Timee_df = color_profit_for_df(mt4_HK_CopyTrade_Open_Time, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Open_Time = mt5_HK_CopyTrade_Open_Time_unsync.result()
    mt5_HK_CopyTrade_Open_Time_df = color_profit_for_df(mt5_HK_CopyTrade_Open_Time, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)


    if "Merging Ticket" in mt4_HK_CopyTrade_Open_Timee_df and "Merging Ticket" in mt5_HK_CopyTrade_Open_Time_df:
        table_5_df = mt4_HK_CopyTrade_Open_Timee_df.merge(mt5_HK_CopyTrade_Open_Time_df, how="left", left_on="Merging Ticket", right_on = "Merging Ticket")
        table_5_col = ['Ticket', 'Symbol', 'Net Lots', 'CFH Net Lots', 'BIC Net Lots', 'SQ Net Lots', \
                       'Philip Net Lots', 'Open Time', 'CFH Open Time', 'BIC Open Time', 'SQ Open Time',
                       'Philip Open Time']
        table_5_df = table_5_df[[c for c in table_5_col if c in table_5_df]]
        table_5_df.fillna("-", inplace=True)

    else:
        table_5_df = pd.DataFrame([{"Run Results": "No Open Trades"}])



    #table_5_df.pop("Merging Ticket")

    #print("6")
    #6th Table
    # ----------------------- MT5 LP Details to look like standadize LP details. ----------------

    lp_details_return_result = lp_details["current_result"]

    # Using MT5 helper function to pretty print the table in HTML.
    mt5_hk_LP_Copy_futures_data = mt5_hk_LP_Copy_futures_data_unsync.result()

    # We want print it into the same structure as mt4 FX LP
    all_lp_details = pretty_print_mt5_futures_LP_details_2(futures_data=mt5_hk_LP_Copy_futures_data, \
                                                           fx_lp_details=lp_details_return_result, return_df=False)
    #print("7")
    # 7th Table
    mt4_HK_CopyTrade_Profit_Comparison_last24hour = mt4_HK_CopyTrade_Profit_Comparison_last24hour_unsync.result()
    mt4_HK_CopyTrade_Profit_Comparison_last24hour_df = color_profit_for_df(mt4_HK_CopyTrade_Profit_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    mt5_HK_CopyTrade_Profit_Comparison_last24hour = mt5_HK_CopyTrade_Profit_Comparison_last24hour_unsync.result()
    mt5_HK_CopyTrade_Profit_Comparison_last24hour_df = color_profit_for_df(mt5_HK_CopyTrade_Profit_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    if "Merging Ticket" in mt4_HK_CopyTrade_Profit_Comparison_last24hour_df and "Merging Ticket" in mt5_HK_CopyTrade_Profit_Comparison_last24hour_df:
        table_7_df = mt4_HK_CopyTrade_Profit_Comparison_last24hour_df.merge(mt5_HK_CopyTrade_Profit_Comparison_last24hour_df, how="left", left_on="Merging Ticket", right_on = "Merging Ticket")
        table_7_df['BGI Profit'] = table_7_df['BGI Profit'] + table_7_df['Philip Profit']

        # to re-color the columns
        table_7_df = color_profit_for_df(table_7_df.to_dict("record"), default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

        table_7_col = ['Ticket', 'Close Time', 'Symbol', 'Net Lots', 'CFH Net Lots',
                       'BIC Net Lots', 'SQ Net Lots', 'Philip Net Lots', 'Profit', 'CFH Profit', 'BIC Profit',
                       'SQ Profit', 'Philip Profit', 'BGI Profit']
        table_7_df = table_7_df[[c for c in table_7_col if c in table_7_df]]

        #table_7_df.pop("Merging Ticket")
        table_7_df.fillna("---", inplace=True)
    else:
        table_7_df = pd.DataFrame([{"Run Results": "No Open Trades"}])



    #print("8")
    # 8th Table
    mt4_HK_CopyTrade_Price_Comparison_last24hour = mt4_HK_CopyTrade_Price_Comparison_last24hour_unsync.result()
    mt4_HK_CopyTrade_Price_Comparison_last24hour_df = color_profit_for_df(mt4_HK_CopyTrade_Price_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Price_Comparison_last24hour = mt5_HK_CopyTrade_Price_Comparison_last24hour_unsync.result()
    mt5_HK_CopyTrade_Price_Comparison_last24hour_df = color_profit_for_df(mt5_HK_CopyTrade_Price_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    if "Merging Ticket" in mt4_HK_CopyTrade_Price_Comparison_last24hour_df and "Merging Ticket" in mt5_HK_CopyTrade_Price_Comparison_last24hour_df:
        table_8_df = mt4_HK_CopyTrade_Price_Comparison_last24hour_df.merge(
            mt5_HK_CopyTrade_Price_Comparison_last24hour_df, how="left", left_on="Merging Ticket", right_on="Merging Ticket")
        # if "Merging Ticket" in table_8_df:
        #     table_8_df.pop("Merging Ticket")

        table_8_col = ['Ticket', 'Close Time ', 'Symbol', 'Open Price', 'CFH Open Price',
         'BIC Open Price', 'SQ Open Price', 'Close Price', 'Philip Open Price', 'CFH Close Price',
         'BIC Close Price', 'SQ Close Price', 'Philip Close Price' ]

        table_8_df = table_8_df[[c for c in table_8_col if c in table_8_df]]

        table_8_df.fillna("---", inplace=True)
    else:
        table_8_df = pd.DataFrame([{"Run Results": "No Open Trades"}])


    # 9th Table

    mt4_HK_CopyTrade_Open_Time_Comparison = mt4_HK_CopyTrade_Open_Time_Comparison_unsync.result()
    mt4_HK_CopyTrade_Open_Time_Comparison_df = color_profit_for_df(mt4_HK_CopyTrade_Open_Time_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Open_Time_Comparison = mt5_HK_CopyTrade_Open_Time_Comparison_unsync.result()
    mt5_HK_CopyTrade_Open_Time_Comparison_df = color_profit_for_df(mt5_HK_CopyTrade_Open_Time_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    if "Merging Ticket" in mt4_HK_CopyTrade_Open_Time_Comparison_df and "Merging Ticket" in mt5_HK_CopyTrade_Open_Time_Comparison_df:
        table_9_df = mt4_HK_CopyTrade_Open_Time_Comparison_df.merge(mt5_HK_CopyTrade_Open_Time_Comparison_df, how="outer", left_on="Merging Ticket", right_on = "Merging Ticket")
        table_9_df.pop("Merging Ticket")
    else:
        table_9_df = pd.DataFrame([{"Run Results": "No Open Trades"}])

    if "Merging Ticket" in mt4_HK_CopyTrade_Open_Time_Comparison_df and "Merging Ticket" in mt5_HK_CopyTrade_Open_Time_Comparison_df:
        table_9_df = mt4_HK_CopyTrade_Open_Time_Comparison_df.merge(mt5_HK_CopyTrade_Open_Time_Comparison_df,
                                                                    how="left", left_on="Merging Ticket",
                                                                    right_on="Merging Ticket")
        table_9_df.pop("Merging Ticket")
        table_9_col = ['Ticket', 'Symbol', 'Open Time', 'CFH Open Time', 'BIC Open Time',
         'SQ Open Time','Philip Open Time', 'Close Time', 'CFH Close Time', 'BIC Close Time',
         'SQ Close Time', 'Philip Close Time']

        table_9_df = table_9_df[[c for c in table_9_col if c in table_9_df]]

        table_9_df.fillna("---", inplace=True)
    else:
        table_9_df = pd.DataFrame([{"Run Results": "No Open Trades"}])

    # print("table_9_df")
    # print(table_9_df.columns)

    # print({ "Hss1": table_1_concat_return_data,
    #                     "Vss1": [table_2_return_data],
    #                     "Vss2":table_3_return_data,
    #                     "H1": table_4_df.to_dict("record"),
    #                     "H2": table_5_df.to_dict("record"),
    #                     "H3" : all_lp_details,
    #                     "H4": table_7_df.to_dict("record"),
    #                     "H5": table_8_df.to_dict("record"),
    #                     "H6": table_9_df.to_dict("record"),
    #                     })


    #print(table_4_df)

    return json.dumps({ "Hss1": table_1_concat_return_data,
                        "Vss1": [table_2_return_data],
                        "Vss2" : table_3_return_data,
                        "H1": table_4_df.to_dict("record"),
                        "H2": table_5_df.to_dict("record"),
                        "H3": all_lp_details,
                        "H4": table_7_df.to_dict("record"),
                        "H5": table_8_df.to_dict("record"),
                        "H6": table_9_df.to_dict("record"),
                        })


    # return json.dumps({ "Hss1": table_1_concat_return_data,
    #                     "Vss1": [table_2_return_data],
    #                     "Vss2":table_3_return_data,
    #                     "H1": table_4_df.to_dict("record"),
    #                     "H2": table_5_df.to_dict("record"),
    #                     "H3" : all_lp_details,
    #                     "H4": table_7_df.to_dict("record"),
    #                     "H5": table_8_df.to_dict("record"),
    #                     "H6": table_9_df.to_dict("record"),
    #                     })


# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@mt5_monitoring.route('/AIF_AB_Hedge', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Risk_UK"])
def AIF_AB_Hedge():
    header = Markup("<b><u>AIF A/B Hedge</u></b>")
    title = "AIF A/B Hedge"

    description = Markup("""Checks for UK A/B Hedge<br>All Time are in SGT (GMT + 8).<br>Refresh timing would be ~ 5 mins as per MT5 Refresh timings.
                            <br>Scope LP : 80004""")

    return render_template("Wbwrk_Multitable_Borderless_redalert.html",
                           backgroud_Filename=background_pic("AIF_AB_Hedge"), icon=icon_pic("UK_AB_Hedge"),
                           Table_name={#"TOTAL CLOSED PNL": "Hss1",
                                       # "FLOATING LOTS": "Vss1",
                                       # "FLOATING PROFIT": "Vss2",
                                       "Account Details": "H1",
                                       "Open Position Tally": "H2",
                                       "LP Details": "H3",
                                       # "Line1": "Hr1",
                                       # "[CLOSED] LOT/PROFIT COMPARISON": "H4",
                                       # "[CLOSED] OPEN/CLOSE PRICE COMPARISON": "H5",
                                       # "[CLOSED] OPEN/CLOSE TIME COMPARISON": "H6"
                                       },
                           title=title, setinterval=200,
                           ajax_url=url_for('mt5_monitoring.AIF_AB_Hedge_ajax', _external=True),
                           header=header,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["mismatch", "so alert:", "alert"]))  # setinterval=60,



@mt5_monitoring.route('/AIF_AB_Hedge_ajax', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Dealing", "Risk_UK"])
def AIF_AB_Hedge_ajax(update_tool_time=0):    # To upload the Files, or post which trades to delete on MT5


    testing = False
    # Crafting the Email.
    email_tile = "UK AIF A/B Hedging Mismatch"
    email_html = "Hi,<br><br>There's a mismatch for the UK copy trades (UK AIF Hedging A/B).<br>"

    ## SQL update statement, to update after email has been sent
    email_sent_Netvol_sql_update_statement = ""
    email_sent_Scope_sql_update_statement = ""

    if testing:
        pd.set_option("display.max_rows", 101)
        pd.set_option("display.max_columns", 101)

    # After how many mins will a mismatch be sent
    alert_mismatch_timing = 10 if testing == False else 5

    Send_Email_Alert_Flag = False

    # All the SQL that we need to call. CAll them first.
    mt5_Acc_trades_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.aif_netvolume_2()", unsync_app=current_app._get_current_object())
    mt5_Acc_details_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.aif_account_info()", unsync_app=current_app._get_current_object())

    mt5_LP_details_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.UK_LP_Details()", unsync_app=current_app._get_current_object())

    mt5_Acc_details = mt5_Acc_details_unsync.result()
    mt5_Acc_details_df = color_profit_for_df(mt5_Acc_details, default=[{"Run Results": "No Open Trades"}], words_to_find=["pnl"], return_df=True)


    #mt5_Acc_details_df.rename(columns={}, inplace=True)
    #print(mt5_Acc_details_df)
    table_1_concat_return_data = mt5_Acc_details_df.to_dict("records")


    mt5_Acc_trades = mt5_Acc_trades_unsync.result()
    # print(mt5_Acc_trades)

    mt5_Acc_trades_df = color_profit_for_df(mt5_Acc_trades, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    # print(mt5_Acc_trades_df)

    columns = mt5_Acc_trades_df.columns

    # print(mt5_Acc_trades_df)

    # Columns that we need to rename and also add together.
    net_vol_columns = [c for c in list(columns) if c.find("NetVol") > 0]

    #print(list(columns))

    # ['BaseSymbol', '80001_NetVol', '80002_NetVol', '80003_NetVol', '80004_NetVol', 'TotalNetVol', 'Past Netvol Discrepancy', 'Past Netvol Discrepancy Time']

    # mt5_Acc_trades_df

    if testing == True:
        #Artificially create a mismatch
        #pass

        mt5_Acc_trades_df.loc[mt5_Acc_trades_df.BaseSymbol.isin([".DE30", ".JP225"]), "Past Netvol Discrepancy"] = 0.15
        mt5_Acc_trades_df.loc[mt5_Acc_trades_df.BaseSymbol.isin(["EURUSD", "XAUUSD", "USDJPY"]), "TotalNetVol"] = 1
        mt5_Acc_trades_df.loc[mt5_Acc_trades_df.BaseSymbol.isin(["AUDUSD"]), "TotalNetVol"] = 0.35



    # Raise an alert if there's a mismatch.
    if "TotalNetVol" in mt5_Acc_trades_df:
        mismatched_symbol = mt5_Acc_trades_df[mt5_Acc_trades_df["TotalNetVol"] != 0]
        # print(mismatched_symbol["Past Netvol Discrepancy Time"])

        # The symbols that has a mismatch for the first time.
        mismatched_symbol_first = mismatched_symbol[mismatched_symbol["Past Netvol Discrepancy Time"].isna()]

        if len(mismatched_symbol_first) > 0 : # Want to save to SQL the time.
            # To add in the ""
            mismatched_symbol_first["BaseSymbol"] = mismatched_symbol_first["BaseSymbol"].apply(lambda x: "'{}'".format(x))
            mismatched_symbol_first["TotalNetVol"] = mismatched_symbol_first["TotalNetVol"].apply(lambda x: "{:.2f}".format(x))

            # Preparing the SQL Values for insert
            value_list = mismatched_symbol_first[["BaseSymbol", "TotalNetVol"]].to_records(index=False)
            value_list = [list(c) + ["NOW()", "'Y'", "'Net_Volume_Mismatch'"] for c in value_list] ## Adding Default values.
            value_list = ["({})".format(" , ".join(c)) for c in value_list]

            SQL_insert = """INSERT INTO aaron.aif_position_mismatch_records (Basesymbol, Discrepancy, Datetime, Flag, Alert_Type) 
                        VALUES {}""".format(", ".join(value_list))
            #print(SQL_insert)
            SQL_insert_MT5_statement(SQL_insert)

        # print(mismatched_symbol)
        # print("\n\n")

        # Those symbols that has mismatched before in the past.
        mismatched_symbol_past = mismatched_symbol[~mismatched_symbol["Past Netvol Discrepancy Time"].isna()]

        # Want those which alert we have not sent.
        # if "Alert_Raised" != "1970-01-01 00:00:00"
        mismatched_symbol_past = mismatched_symbol_past[mismatched_symbol_past["Alert_Raised"] == "1970-01-01 00:00:00"]
        # print("\n\n")
        # print(mismatched_symbol_past)


        if len(mismatched_symbol_past) > 0:  # Want to check the time, if matches condition, send the alert
            mismatched_symbol_past['Past Netvol Discrepancy Time'] = pd.to_datetime(mismatched_symbol_past['Past Netvol Discrepancy Time'],
                                                                        format='%Y-%m-%d %H:%M:%S')

            mismatched_symbol_past["Time_diff"] = pd.datetime.now() - mismatched_symbol_past["Past Netvol Discrepancy Time"]
            mismatched_symbol_past["Time_diff_m"] = mismatched_symbol_past["Time_diff"].dt.total_seconds() / 60 # Calculate the total mins

            print(mismatched_symbol_past)
            # Past a certain time, we want to send an alert.
            To_Send_alert = mismatched_symbol_past[mismatched_symbol_past["Time_diff_m"] >= alert_mismatch_timing]

            for c in ["Time_diff", "Time_diff_m" , "Alert_Raised"]:
                if c in  To_Send_alert:
                    To_Send_alert.drop(c, inplace=True, axis=1)


            # If it fufils the sending alert conditions.
            if len(To_Send_alert) > 0:
                print("AIF: Will need to send alert. ")
                Send_Email_Alert_Flag = True

                mismatch_list = To_Send_alert["BaseSymbol"].to_list()
                #print(mismatch_list)
                # Crafting message for telegram.
                #print("BaseSymbol" + net_vol_columns)
                To_Send_alert["tele_Message"] = To_Send_alert.apply(lambda x: "{:^6} | {} ".format( \
                                  x["BaseSymbol"], "| ".join(["{:^7.2f}".format(x[c]) for c in net_vol_columns])
                ) , axis=1)

                tele_table_header = ["Symbol"] + [c.replace("NetVol", "") for c in net_vol_columns]
                tele_table_header = ["| ".join(tele_table_header)]

                tele_table_data = To_Send_alert["tele_Message"].tolist()
                tele_table = "\n".join(tele_table_header + tele_table_data)
                # print(tele_table_header)
                # print(To_Send_alert["tele_Message"].tolist())

                if testing == False:
                    # Want to send out an email/telegram alert for mismatch.
                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, """<b>UK Mismatch.</b>\n\n{}\n\nLink: <a href='{}'>UK Hedge Page</a>""".format(tele_table,\
                                            url_for('mt5_monitoring.AIF_AB_Hedge', _external=True).replace("localhost", EXTERNAL_IP)), \
                                           TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)

                # Don't need this column anymore.
                To_Send_alert.drop("tele_Message", inplace=True, axis=1)

                # Don't want any of the scope LP details here
                To_Send_alert = To_Send_alert[[c for c in To_Send_alert.columns if c.lower().find("scope") == -1]]


                email_html += """<br><b><u>Net Volume Mismatch</u></b><br>
                The net volume of all the accounts is not zero.<br>
                Accounts 80001 should hedge off Account 80002.<br>
                Account 80003 should tally off with Account 80004<br>
                The accounts are not fully hedged.<br>Kindly see details below.<br>"""
                email_html += Array_To_HTML_Table([c.replace("_", " ") for c in To_Send_alert.columns], To_Send_alert.values, [])
                email_html += "Ideally, the position should net off completely, resulting in 0 net volume.<br><br>"


                # Need to update SQL so that Alert dosn't send again.
                mismatch_list = ["'{}'".format(m) for m in mismatch_list] # Add quotations
                email_sent_Netvol_sql_update_statement = """UPDATE aaron.aif_position_mismatch_records SET Alert_Raised = NOW() 
                                       WHERE Basesymbol in ({}) AND Flag = 'Y' AND Alert_Type = "Net_Volume_Mismatch" """.format(", ".join(mismatch_list))

                # if testing == False:

                #print(sql_update_statement)
                # print(To_Send_alert)



    # We want to clear the past netvolume mismatches.
    if 'Past Netvol Discrepancy' in mt5_Acc_trades_df and 'TotalNetVol' in mt5_Acc_trades_df:
        cleared_mismatch_df = mt5_Acc_trades_df[(mt5_Acc_trades_df['Past Netvol Discrepancy'] != 0) & (mt5_Acc_trades_df['TotalNetVol'] == 0) ]
        if len(cleared_mismatch_df) != 0: # We want to clear the mismatch in the mismatch table on MT5 SQL
            symbols_to_clear = cleared_mismatch_df["BaseSymbol"].tolist()
            symbols_to_clear = ['"{c}"'.format(c=c) for c in symbols_to_clear]
            print("\nWant to clear out the AIF A/B mismatches. \n")
            # Want to set the Flag to N, meaning that the mismatch is no longer active.
            clear_sql_statement = """UPDATE aaron.aif_position_mismatch_records SET Flag = "N" WHERE Basesymbol in ({}) 
                                    AND Flag="Y" and Alert_Type="Net_Volume_Mismatch" """.format(" , ".join(symbols_to_clear))

            if testing == False:
                SQL_insert_MT5_statement(clear_sql_statement)
                #print(clear_sql_statement)


    # mt5_LP_details = mt5_LP_details_unsync.result()
    # print(mt5_LP_details)

    # we want to check if there's any 80004 and Scope Mismatch.
    # First, see if the columns are inside.
    if all([c in mt5_Acc_trades_df for c in ["80004_NetVol", "Scope_NetVol"]]):

        if testing == True:
            # Artificially create a scope LP mismatch
            mt5_Acc_trades_df.loc[mt5_Acc_trades_df.BaseSymbol.isin(["EURUSD"]), "Scope_NetVol"] = 0.15
            mt5_Acc_trades_df.loc[mt5_Acc_trades_df.BaseSymbol.isin(["XAUUSD"]), "Scope_NetVol"] = 0.15



        # Next, we want to create a column that is the net of them both,
        # 80004 should have the same volume as Scope.
        mt5_Acc_trades_df["80004-scope"] = mt5_Acc_trades_df["80004_NetVol"] - mt5_Acc_trades_df["Scope_NetVol"]
        scope_mismatch_df = mt5_Acc_trades_df[mt5_Acc_trades_df["80004-scope"] != 0]

        # print(scope_mismatch_df)

        # The symbols that has a mismatch for the first time.
        scope_mismatched_symbol_first = scope_mismatch_df[scope_mismatch_df["Past Scope Discrepancy Time"].isna()]

        # print("First time Scope mismatch")
        # print(scope_mismatched_symbol_first)

        if len(scope_mismatched_symbol_first) > 0:  # If there are NEW scope mismatches.

            # For new mismatches, we want to add it into our database.

            scope_mismatched_symbol_first["BaseSymbol"] = scope_mismatched_symbol_first["BaseSymbol"].apply(lambda x: "'{}'".format(x))    # To add in the ""
            scope_mismatched_symbol_first["80004-scope"] = scope_mismatched_symbol_first["80004-scope"].apply(lambda x: "{:.2f}".format(x))

            # Preparing the SQL Values for insert

            value_list = scope_mismatched_symbol_first[["BaseSymbol", "80004-scope"]].to_records(index=False)
            value_list = [list(c) + ["NOW()", "'Y'", "'Scope_Mismatch'"] for c in value_list]  ## Adding Default values.
            value_list = ["({})".format(" , ".join(c)) for c in value_list]

            SQL_insert = """INSERT INTO aaron.aif_position_mismatch_records (Basesymbol, Discrepancy, Datetime, Flag, Alert_Type) 
                        VALUES {}""".format(", ".join(value_list))

            # print(SQL_insert)
            SQL_insert_MT5_statement(SQL_insert)



            # Want to inform of mismatch if it's past the threshold timings.


        # Those symbols that has mismatched before in the past.
        scope_mismatched_symbol_past = scope_mismatch_df[~scope_mismatch_df["Past Scope Discrepancy Time"].isna()]

        # Want those which alert we have not sent.
        # if "Alert_Raised" != "1970-01-01 00:00:00"
        scope_mismatched_symbol_past = scope_mismatched_symbol_past[scope_mismatched_symbol_past["Scope Alert Raised"] == "1970-01-01 00:00:00"]

        scope_mismatched_symbol_past['Past Scope Discrepancy Time'] = pd.to_datetime(scope_mismatched_symbol_past['Past Scope Discrepancy Time'],
            format='%Y-%m-%d %H:%M:%S')

        scope_mismatched_symbol_past["Time_diff"] = pd.datetime.now() - scope_mismatched_symbol_past["Past Scope Discrepancy Time"]
        scope_mismatched_symbol_past["Time_diff_m"] = scope_mismatched_symbol_past["Time_diff"].dt.total_seconds() / 60  # Calculate the total mins


        To_Send_alert = scope_mismatched_symbol_past[scope_mismatched_symbol_past["Time_diff_m"] >= alert_mismatch_timing]

        if len(To_Send_alert) > 0:
            Send_Email_Alert_Flag = True


            # We only want the columns that are in the df
            needed_columns = [c for c in ['BaseSymbol', '80004_NetVol', 'Scope_NetVol', 'Past Scope Discrepancy Time', '80004-scope'] if c in To_Send_alert]

            # print("We need to raise an alert for scope mismatch: {} ".format(value_list))
            email_html += "<b><u>80004 - Scope Mismatch</u></b><br>Scope(LP) Should directly reflect the position in Account 80004.<br>However, there's a mismatch.<br>Kindly see details below.<br>"
            email_html += Array_To_HTML_Table([c.replace("_", " ") for c in needed_columns], To_Send_alert[needed_columns].values, [])

            # Need to update SQL that the alert has been sent, so we don't need to send it again.
            # Preparing the data to be sent.
            mismatch_list = To_Send_alert["BaseSymbol"].to_list()
            mismatch_list = ["'{}'".format(c) for c in mismatch_list]    # To make it SQL friendly.

            email_sent_Scope_sql_update_statement = """UPDATE aaron.aif_position_mismatch_records SET Alert_Raised = NOW() 
                WHERE Basesymbol in ({}) AND Flag = 'Y' AND Alert_Type = "Scope_Mismatch" """.format(
                ", ".join(mismatch_list))





        # print(mt5_Acc_trades_df)


        # Want to clear the old mismatch, if it's been cleared.
        if 'Past Scope Discrepancy Time' in mt5_Acc_trades_df and \
                '80004-scope' in mt5_Acc_trades_df:

            print("Checking for Scope Finished Mismatch")

            cleared_scope_mismatch_df = mt5_Acc_trades_df[
                (~mt5_Acc_trades_df['Past Scope Discrepancy Time'].isna()) & (mt5_Acc_trades_df['80004-scope'] == 0)]


            if len(cleared_scope_mismatch_df) != 0:  # We want to clear the mismatch in the mismatch table on MT5 SQL
                symbols_to_clear = cleared_scope_mismatch_df["BaseSymbol"].tolist()
                symbols_to_clear = ['"{c}"'.format(c=c) for c in symbols_to_clear]
                print("\nWant to clear out the AIF Scope A/B mismatches. \n")
                # Want to set the Flag to N, meaning that the mismatch is no longer active.
                clear_sql_statement = """UPDATE aaron.aif_position_mismatch_records SET Flag = "N" WHERE Basesymbol in ({}) 
                                        AND Flag="Y" and Alert_Type="Scope_Mismatch" """.format(" , ".join(symbols_to_clear))
                print(clear_sql_statement)
                SQL_insert_MT5_statement(clear_sql_statement)






    # Get the LP details.
    df_mt5_LP_details = pd.DataFrame(mt5_LP_details_unsync.result())

    # print(df_mt5_LP_details)

    # To ensure that it's printed to
    to_round_columns = ['Deposit', 'Credit', 'PnL', 'Equity', 'Total_margin', 'Free_margin', 'EQUITY', 'Margin/Equity (%)',  'available']
    for c in to_round_columns:
        if c in df_mt5_LP_details:
            df_mt5_LP_details[c] = df_mt5_LP_details[c].apply(lambda x: round(float(x), 2) if isfloat(x) else None )

    if "LP" in df_mt5_LP_details:   # Don't want to see the underscore. Replace it with a space.
        df_mt5_LP_details["LP"] = df_mt5_LP_details["LP"].apply(lambda x: str(x).replace("_", " "))



    lp_details_data  = df_mt5_LP_details.to_dict("records") # Original data.
    #lp_details_return_data = lp_details_data
    lp_details_return_data = []  # We want to add and re-configure these data
    #'Deposit', 'Credit', 'PnL', 'Equity', 'Total_margin', 'Free_margin', 'EQUITY', 'Margin/Equity (%)',  'available'

    for d in lp_details_data:
        loop_data = dict()
        loop_data["LP"] = d["LP"] if "LP" in d else None

        loop_data["BALANCE"] = dict()
        loop_data["BALANCE"]["DEPOSIT"] = "$ {:,.2f}".format(float(d["Deposit"])) if "Deposit" in d else None



        loop_data["BALANCE"]["CREDIT"] = "$ {:,.2f}".format(float(d["Credit"])) if "Credit" in d else None
        loop_data["BALANCE"]["PNL"] = d["PnL"] if "PnL" in d else None
        loop_data["BALANCE"]["EQUITY"] = "$ {:,.2f}".format(float(d["EQUITY"]))  if "Equity" in d else None

        loop_data["MARGIN"] = dict()
        loop_data["MARGIN"]["TOTAL MARGIN"] = d["Total_margin"] if "Total_margin" in d else None
        loop_data["MARGIN"]["FREE MARGIN"] = "$ {:,.2f}".format(float(d["Free_margin"])) if "Free_margin" in d else None

        loop_data["MARGIN/EQUITY (%)"] = d["Margin/Equity (%)"] if "Margin/Equity (%)" in d else None


        loop_data["MC/SO/AVAILABLE"] = dict()
        loop_data["MC/SO/AVAILABLE"]["MARGIN CALL (M/E)"] = d['margin_call (M/E)'] if "margin_call (M/E)" in d else None
        loop_data["MC/SO/AVAILABLE"]["STOP OUT (M/E)"] = d['stop_out (M/E)']  if "stop_out (M/E)" in d else None
        loop_data["MC/SO/AVAILABLE"]["STOP OUT AMOUNT (M/E)"] =  "$ {:,.2f}".format(float(d["STOPOUT AMOUNT"])) if "STOPOUT AMOUNT" in d else None
        loop_data["MC/SO/AVAILABLE"]["AVILABLE"] = "-"

        loop_data["UPDATED TIME"] = d["Updated_time"] if "Updated_time" in d else None
        lp_details_return_data.append(loop_data)


    # loop_buffer["BALANCE"] = dict()
    # loop_buffer["BALANCE"]["DEPOSIT"] = "$ {:,.2f}".format(float(lp["deposit"])) if "deposit" in lp else None
    # loop_buffer["BALANCE"]["CREDIT"] = "$ {:,.2f}".format(float(lp["credit"])) if "credit" in lp else None
    #
    #

    if Send_Email_Alert_Flag == True:
        email_list = ["aaron.lim@blackwellglobal.com"] if testing == True else EMAIL_LIST_ALERT + [Risk_EU_EMAIL,
                                                                                                   "risk@blackwellglobal.bs"]

        email_html += """Link: <a href='{}'>UK AIF Hedge Page</a><br><br>""".format(url_for('mt5_monitoring.AIF_AB_Hedge',
                                                    _external=True).replace("localhost", EXTERNAL_IP))

        email_html += "This Email was generated at: SGT {}.<br><br>Thanks,<br>Risk SG".format( \
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


        async_send_email(email_list, [], email_tile, Email_Header + email_html + Email_Footer, [])

        # Will update onto SQL that we've sent the email.
        if email_sent_Scope_sql_update_statement != "":
            SQL_insert_MT5_statement(email_sent_Scope_sql_update_statement)
        if email_sent_Netvol_sql_update_statement != "":
            SQL_insert_MT5_statement(email_sent_Netvol_sql_update_statement)



    # -----------------------------Pretty Print. -------------------------------------------------------------------

    # print(mt5_Acc_trades_df)
    # print(mt5_Acc_trades_df.columns)

    # Fill na since if there isn't Past Netvol Discrepancy Time, it would be null
    if "Past Netvol Discrepancy Time" in mt5_Acc_trades_df:
        mt5_Acc_trades_df["Past Netvol Discrepancy Time"] = mt5_Acc_trades_df["Past Netvol Discrepancy Time"].fillna(value="-")

    # Fill na since if there isn't Past Scope Discrepancy Time, it would be null
    if "Past Scope Discrepancy Time" in mt5_Acc_trades_df:
        mt5_Acc_trades_df["Past Scope Discrepancy Time"] = mt5_Acc_trades_df["Past Scope Discrepancy Time"].fillna(value="-")



    # Don't need to show this column.
    if "Alert_Raised" in mt5_Acc_trades_df:
        mt5_Acc_trades_df.drop("Alert_Raised", axis=1, inplace=True)


    # Don't need to show this column.
    if "Scope Alert Raised" in mt5_Acc_trades_df:
        mt5_Acc_trades_df.drop("Scope Alert Raised", axis=1, inplace=True)


    # Want to tabulate all the NetVol.
    # First, get the sum of the abs volume.
    mt5_Acc_trades_df["abs_volume"] = mt5_Acc_trades_df.apply(lambda x: sum([abs(x[c]) \
                                                                             for c in net_vol_columns]), axis=1)
    # Only want to show the position that has open trades.
    open_trades_df = mt5_Acc_trades_df[mt5_Acc_trades_df["abs_volume"] != 0]

    if len(open_trades_df) > 0:  # If there are open position
        mt5_Acc_trades_df = open_trades_df
    else:  # If not, we want to show the major few.
        mt5_Acc_trades_df = mt5_Acc_trades_df[mt5_Acc_trades_df["BaseSymbol"].isin(["EURUSD", "USDCHF", "EURCAD"])]

    # Want to sort to show the mismatches.
    mt5_Acc_trades_df.sort_values(["abs_volume", "TotalNetVol", "Past Netvol Discrepancy", "BaseSymbol"],  ascending=[False, False, False, False], inplace=True)

    # Don't need to show this column.
    if "abs_volume" in mt5_Acc_trades_df:
        mt5_Acc_trades_df.drop("abs_volume", axis=1, inplace=True)


    # Want to pretty print. From NetVol to Net Vol
    to_replace = {c:c.replace("NetVol", " Net Vol") for c in net_vol_columns }
    mt5_Acc_trades_df.rename(columns=to_replace, inplace=True)

    #print(mt5_Acc_trades_df)
    Acc_trades_return_data = mt5_Acc_trades_df.to_dict("records")

    # Want to update the runtime table to ensure that tool is running.
    async_update_Runtime(app=current_app._get_current_object(),
                         Tool='AIF Hedge A/B Match')
    # print("4")
    # print(table_1_concat_return_data)
    # print(Acc_trades_return_data)
    # print(lp_details_return_data)

    return json.dumps({"H1": table_1_concat_return_data,
                       "H2": Acc_trades_return_data,
                       "H3": lp_details_return_data,
                       })

