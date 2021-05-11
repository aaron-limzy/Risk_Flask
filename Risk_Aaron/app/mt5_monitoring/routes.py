

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
                           icon=icon_pic("Futures_LP_Details") ,Table_name="Futures LP Details", title=title,
                           ajax_url=url_for('mt5_monitoring.Futures_LP_Details_Ajax', _external=True), header=header,
                           setinterval=15, no_backgroud_Cover=True,
                           description=description, replace_words=Markup(["Today"]))





@mt5_monitoring.route('/mt5_futures_LP_data_Ajax', methods=['GET', 'POST'])
@roles_required()
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


# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@mt5_monitoring.route('/HK_Copy_STP', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Dealing"])
def HK_Copy_STP():


    header = Markup("<b><u>HK A Book</u></b>")
    title =  "HK A Book"

    description = Markup("""<b>DESCRIPTION</b><br><br>The <span style="color:green">BGI position table </span>  reflects the floating position of the main account being STP <b>by symbol</b><br>
        -The lots being STP will be rounded up to the nearest <span style="color:red">2 decimal places</span>. For example,<br>
        <span style="color:red"><u>25% of 1.5 lots  =0.375 lot, the trade will be hit out as 0.38 lots</u></span>.<br>
        -<span style="color:blue">Net Lots * STP% </span> <br> is calculate on each individual trades hit out to LPs as each trades will be rounded off to the nearest 2 decimal places.
        <br><br>
        <span style="color:green">The Vantage, BIC, and SwissQuote tables</span> shows our floating positions in our LP <b>by symbol</b><br>
        -<span style="color:blue">Lot%</span>: shows the percentage of LP's lot as compared to the main account's lot being copied
        <br><br>
        <span style="color:green">LP Details table</span> will show our current funding in our LPs.<br>
        <br><br>
        <span style="color:green">Trades table</span> will show individual floating trades made by the account being STP as well as its price comparison against trades made in LPs.<br>
        <br>
        <b>SQL INFO</b><br>
        <br>
        The account being STP,STP % and Vantage trade copier's login can be set in sql table [aaron.aaron_misc_data] <br>
        <ul>
        <li>item=HK_Copy_Trade_Main_Login </li>
        <li>item = HK_Copy_Trade_Vantage_Login</li>
        <li>item = HK_Copy_Trade_Percentage</li>
        <li>item = HK_Copy_Trade_Percentage_Vantage</li>
        <li>item = HK_Copy_Trade_Percentage_BIC</li>
        <li>item = HK_Copy_Trade_Percentage_SQ</li>
        </ul>
        <br>
        The trades information can be take from 
        <ul>
        <li>Vantage -> MT4 live 1 </li>
        <li>BIC -> aaron.live_trade_88803164_b.i.c.markets_live </li>
        <li>SwissQuote -> aaron.live_trade_714009_swissquote_live </li>
        </ul>""")


    return render_template("Wbwrk_Multitable_Borderless_redalert.html", backgroud_Filename=background_pic("HK_Copy_STP"), icon=icon_pic("ABook_BGI"),
                           Table_name={"TOTAL CLOSED PNL": "Hss1",
                                       "FLOATING LOTS": "Vss1",
                                       "FLOATING PROFIT": "Vss2",
                                       "[OPEN] PRICE&PROFIT COMPARISON": "H1",
                                       "[OPEN] Lots& Open Time Comparison": "H2",
                                       "LP Details": "H3",
                                       "[OPEN] PRICE&PROFIT COMPARISON": "H4",
                                       "Line1"                           : "Hr1" ,
                                       "[CLOSED] LOT/PROFIT COMPARISON": "H5",
                                       "[CLOSED] OPEN/CLOSE PRICE COMPARISON": "H6",
                                       "OPEN/CLOSE TIME COMPARISON"         : "H7"
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


    # First table
    # MT4 + MT5 Copy Trades Total PnL
    mt4_HK_CopyTrade_Total_Pnl_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Total_Pnl()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Total_Pnl_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Total_Pnl()", unsync_app=current_app._get_current_object())


    # Second table
    #[HK_CopyTrade_NetLots_Difference]+MT5[HK_CopyTrade_NetLots_Difference]
    mt4_HK_CopyTrade_NetLots_Difference_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_NetLots_Difference()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_NetLots_Difference_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_NetLots_Difference()", unsync_app=current_app._get_current_object())


    # Third table
    mt4_HK_CopyTrade_Profit_Difference_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Profit_Difference()", app=current_app._get_current_object())


    # 4th table
    mt4_HK_CopyTrade_Price_Comparison_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Price_Comparison_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison()", unsync_app=current_app._get_current_object())


    # 5th Table
    mt4_HK_CopyTrade_Open_Time_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Open_Time_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison()", unsync_app=current_app._get_current_object())



    # 7th Table
    #[HK_CopyTrade_Profit_Comparison_last24hour] + MT5.[HK_CopyTrade_Profit_Comparison_last24hour]

    mt4_HK_CopyTrade_Profit_Comparison_last24hour_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Profit_Comparison_last24hour()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Profit_Comparison_last24hour_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Profit_Comparison_last24hour()", unsync_app=current_app._get_current_object())


    # 8th Table
    # [HK_CopyTrade_Price_Comparison_last24hour] + MT5.[HK_CopyTrade_Price_Comparison_last24hour]
    mt4_HK_CopyTrade_Price_Comparison_last24hour_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison_last24hour()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Price_Comparison_last24hour_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Price_Comparison_last24hour()", unsync_app=current_app._get_current_object())


    # 9th Table
    #[HK_CopyTrade_Open_Time_Comparison] + MT5.[HK_CopyTrade_Open_Time_Comparison]
    mt4_HK_CopyTrade_Open_Time_Comparison_unsync = unsync_query_SQL_return_record_fun(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison()", app=current_app._get_current_object())
    mt5_HK_CopyTrade_Open_Time_Comparison_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="call aaron.HK_CopyTrade_Open_Time_Comparison()", unsync_app=current_app._get_current_object())




    # 6th Table
    # # While waiting, we will call somthing that isn't unsync
    lp_details = ABook_LP_Details_function(exclude_list=["CFH", "GlobalPrime", "demo"])
    mt5_hk_LP_Copy_futures_data_unsync = mt5_HK_CopyTrade_Futures_LP_data(unsync_app=current_app._get_current_object())


    # ----------- To get the results ------

    mt4_HK_CopyTrade_Total_Pnl = mt4_HK_CopyTrade_Total_Pnl_unsync.result()
    mt4_HK_CopyTrade_Total_Pnl_df = color_profit_for_df(mt4_HK_CopyTrade_Total_Pnl, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Total_Pnl = mt5_HK_CopyTrade_Total_Pnl_unsync.result()
    mt5_HK_CopyTrade_Total_Pnl_df = color_profit_for_df(mt5_HK_CopyTrade_Total_Pnl, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_1_concat = pd.concat([mt4_HK_CopyTrade_Total_Pnl_df, mt5_HK_CopyTrade_Total_Pnl_df])
    table_1_concat_return_data =  table_1_concat.to_dict("record")

    mt4_HK_CopyTrade_Total_Pnl = None
    mt5_HK_CopyTrade_Total_Pnl = None
    mt4_HK_CopyTrade_Total_Pnl_df = None
    mt5_HK_CopyTrade_Total_Pnl_df = None


    # Second table ---
    mt4_HK_CopyTrade_NetLots_Difference = mt4_HK_CopyTrade_NetLots_Difference_unsync.result()
    mt4_HK_CopyTrade_NetLots_Difference_df = color_profit_for_df(mt4_HK_CopyTrade_NetLots_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_NetLots_Difference = mt5_HK_CopyTrade_NetLots_Difference_unsync.result()
    mt5_HK_CopyTrade_NetLots_Difference_df = color_profit_for_df(mt5_HK_CopyTrade_NetLots_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_2_return_data = {}
    if all([c in mt4_HK_CopyTrade_NetLots_Difference_df for c in
            ["Net Lots", "LP Net Lots", "STP%", "LP Net Lots - Net Lots * STP%"]]) and "Philip Net Lots" in mt5_HK_CopyTrade_NetLots_Difference_df:
        table_2_return_data["Net Lots"] = mt4_HK_CopyTrade_NetLots_Difference_df["Net Lots"].sum()
        table_2_return_data["LP Net Lots"] = mt4_HK_CopyTrade_NetLots_Difference_df["LP Net Lots"].sum() + mt5_HK_CopyTrade_NetLots_Difference_df["Philip Net Lots"].sum()
        table_2_return_data["STP%"] = mt4_HK_CopyTrade_NetLots_Difference_df["STP%"].sum()
        table_2_return_data["LP Net Lots - Net Lots * STP%"] = mt4_HK_CopyTrade_NetLots_Difference_df["LP Net Lots - Net Lots * STP%"].sum() + \
                            mt5_HK_CopyTrade_NetLots_Difference_df["Philip Net Lots"].sum()

    # Third table
    mt4_HK_CopyTrade_Profit_Difference = mt4_HK_CopyTrade_Profit_Difference_unsync.result()
    mt4_HK_CopyTrade_Profit_Difference_df = color_profit_for_df(mt4_HK_CopyTrade_Profit_Difference, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)

    table_3_return_data = {}
    if all([c in mt4_HK_CopyTrade_Profit_Difference_df for c in ["LP Profit", "Profit"]]) and "Philip Net Lots" in mt5_HK_CopyTrade_NetLots_Difference_df:
        table_3_return_data["LP Profit"] = mt4_HK_CopyTrade_Profit_Difference_df['LP Profit'].sum() + mt5_HK_CopyTrade_NetLots_Difference_df["Philip Net Lots"].sum()
        table_3_return_data["Profit USD"] = mt4_HK_CopyTrade_Profit_Difference_df["Profit"].sum()
        table_3_return_data["Net Profit"] = table_3_return_data["LP Profit"] - table_3_return_data["Profit USD"]
        # To color the profit columns
        table_3_return_data = color_profit_for_df([table_3_return_data], default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=False)

    # 4th Table
    mt4_HK_CopyTrade_Price_Comparison = mt4_HK_CopyTrade_Price_Comparison_unsync.result()
    mt4_HK_CopyTrade_Price_Comparisone_df = color_profit_for_df(mt4_HK_CopyTrade_Price_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Price_Comparison = mt5_HK_CopyTrade_Price_Comparison_unsync.result()
    mt5_HK_CopyTrade_Price_Comparison_df = color_profit_for_df(mt5_HK_CopyTrade_Price_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_4_df = mt4_HK_CopyTrade_Price_Comparisone_df.merge(mt5_HK_CopyTrade_Price_Comparison_df, how="outer", left_on="Ticket", right_on = "Merging Ticket")
    
    table_4_df.pop("Merging Ticket")


    # 5th Table

    mt4_HK_CopyTrade_Open_Time = mt4_HK_CopyTrade_Open_Time_unsync.result()
    mt4_HK_CopyTrade_Open_Timee_df = color_profit_for_df(mt4_HK_CopyTrade_Open_Time, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Open_Time = mt5_HK_CopyTrade_Open_Time_unsync.result()
    mt5_HK_CopyTrade_Open_Time_df = color_profit_for_df(mt5_HK_CopyTrade_Open_Time, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_5_df = mt4_HK_CopyTrade_Open_Timee_df.merge(mt5_HK_CopyTrade_Open_Time_df, how="outer", left_on="Ticket", right_on = "Merging Ticket")
    #table_5_df.pop("Merging Ticket")


    #6th Table
    # ----------------------- MT5 LP Details to look like standadize LP details. ----------------

    lp_details_return_result = lp_details["current_result"]

    # Using MT5 helper function to pretty print the table in HTML.
    mt5_hk_LP_Copy_futures_data = mt5_hk_LP_Copy_futures_data_unsync.result()

    # We want print it into the same structure as mt4 FX LP
    all_lp_details = pretty_print_mt5_futures_LP_details_2(futures_data=mt5_hk_LP_Copy_futures_data, \
                                                           fx_lp_details=lp_details_return_result, return_df=False)

    # 7th Table
    mt4_HK_CopyTrade_Profit_Comparison_last24hour = mt4_HK_CopyTrade_Profit_Comparison_last24hour_unsync.result()
    mt4_HK_CopyTrade_Profit_Comparison_last24hour_df = color_profit_for_df(mt4_HK_CopyTrade_Profit_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Profit_Comparison_last24hour = mt5_HK_CopyTrade_Profit_Comparison_last24hour_unsync.result()
    mt5_HK_CopyTrade_Profit_Comparison_last24hour_df = color_profit_for_df(mt5_HK_CopyTrade_Profit_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    if "Ticket" in mt4_HK_CopyTrade_Profit_Comparison_last24hour_df and "Merging Ticket" in mt5_HK_CopyTrade_Profit_Comparison_last24hour_df:
        table_7_df = mt4_HK_CopyTrade_Profit_Comparison_last24hour_df.merge(mt5_HK_CopyTrade_Profit_Comparison_last24hour_df, how="outer", left_on="Ticket", right_on = "Merging Ticket")
        #table_7_df.pop("Merging Ticket")
        table_7_df.fillna("---", inplace=True)
    else:
        table_7_df = pd.DataFrame([{"Run Results": "No Open Trades"}])


    # 8th Table
    mt4_HK_CopyTrade_Price_Comparison_last24hour = mt4_HK_CopyTrade_Price_Comparison_last24hour_unsync.result()
    mt4_HK_CopyTrade_Price_Comparison_last24hour_df = color_profit_for_df(mt4_HK_CopyTrade_Price_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Price_Comparison_last24hour = mt5_HK_CopyTrade_Price_Comparison_last24hour_unsync.result()
    mt5_HK_CopyTrade_Price_Comparison_last24hour_df = color_profit_for_df(mt5_HK_CopyTrade_Price_Comparison_last24hour, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    if "Ticket" in mt4_HK_CopyTrade_Price_Comparison_last24hour_df and "Merging Ticket" in mt5_HK_CopyTrade_Price_Comparison_last24hour_df:
        table_8_df = mt4_HK_CopyTrade_Price_Comparison_last24hour_df.merge(
            mt5_HK_CopyTrade_Price_Comparison_last24hour_df, how="outer", left_on="Ticket", right_on="Merging Ticket")
        #table_8_df.pop("Merging Ticket")
        table_8_df.fillna("---", inplace=True)
    else:
        table_8_df = pd.DataFrame([{"Run Results": "No Open Trades"}])



    # 9th Table

    mt4_HK_CopyTrade_Open_Time_Comparison = mt4_HK_CopyTrade_Open_Time_Comparison_unsync.result()
    mt4_HK_CopyTrade_Open_Time_Comparison_df = color_profit_for_df(mt4_HK_CopyTrade_Open_Time_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    mt5_HK_CopyTrade_Open_Time_Comparison = mt5_HK_CopyTrade_Open_Time_Comparison_unsync.result()
    mt5_HK_CopyTrade_Open_Time_Comparison_df = color_profit_for_df(mt5_HK_CopyTrade_Open_Time_Comparison, default=[{"Run Results": "No Open Trades"}], words_to_find=["profit"], return_df=True)

    table_9_df = mt4_HK_CopyTrade_Open_Time_Comparison_df.merge(mt5_HK_CopyTrade_Open_Time_Comparison_df, how="outer", left_on="Ticket", right_on = "Merging Ticket")
    table_9_df.pop("Merging Ticket")

    if "Ticket" in mt4_HK_CopyTrade_Open_Time_Comparison_df and "Merging Ticket" in mt5_HK_CopyTrade_Open_Time_Comparison_df:
        table_9_df = mt4_HK_CopyTrade_Open_Time_Comparison_df.merge(mt5_HK_CopyTrade_Open_Time_Comparison_df,
                                                                    how="outer", left_on="Ticket",
                                                                    right_on="Merging Ticket")
        #table_9_df.pop("Merging Ticket")
        table_9_df.fillna("---", inplace=True)
    else:
        table_9_df = pd.DataFrame([{"Run Results": "No Open Trades"}])


    return json.dumps({ "Hss1": table_1_concat_return_data,
                        "Vss1": [table_2_return_data],
                        "Vss2":table_3_return_data,
                        "H1": table_4_df.to_dict("record"),
                        "H2": table_5_df.to_dict("record"),
                        "H3" : all_lp_details,
                        "H4": table_7_df.to_dict("record"),
                        "H5": table_8_df.to_dict("record"),
                        "H6": table_9_df.to_dict("record"),
                        })



