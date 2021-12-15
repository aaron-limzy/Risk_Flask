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

analysis = Blueprint('analysis', __name__)


# AARON_BOT = "736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0"		# For Mismatch and LP Margin
# TELE_ID_USDTWF_MISMATCH = "776609726:AAHVrhEffiJ4yWTn1nw0ZBcYttkyY0tuN0s"        # For USDTWF
# TELE_CLIENT_ID = ["486797751"]        # Aaron's Telegram ID.

# @analysis.route('/Swaps/BGI_Swaps')
#
# def BGI_Swaps():
#     description = Markup("Swap values uploaded onto MT4/MT5. <br>\
#    Swaps would be charged on the roll over to the next day.<br> \
#     Three day swaps would be charged for FX on weds and CFDs on fri. ")
#
#     return render_template("Standard_Single_Table.html", backgroud_Filename='css/Faded_car.jpg', Table_name="BGI_Swaps", \
#                            title="BGISwaps", ajax_url=url_for("swaps.BGI_Swaps_ajax"),
#                            description=description, replace_words=Markup(["Today"]))
#
#

# Want to write text of num in a shorter way.
def text_numbers(num):
    if abs(num) < 1000:
        return  "$ {:,.2f}".format(num)
    elif abs(num)<1000000:
        return  "$ {:,.2f}k".format(num/1000)
    else:
        return  "$ {:,.2f}M".format(num/1000000)


def create_plot():

    N = 40
    x = np.linspace(0, 1, N)
    y = np.random.randn(N)
    df = pd.DataFrame({'x': x, 'y': y}) # creating a sample dataframe

    data = [
        go.Scatter(
            x=df['x'], # assign x as the dataframe column 'x'
            y=df['y']
        )
    ]
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON



# TW Side SQL
def get_tw_df():

    query_bridge_SQL = """SELECT LOGIN, SYMBOL, CMD, SUM(VOLUME*0.01) as VOLUME, OPEN_TIME, SUM(SWAPS + PROFIT) AS REVENUE, live3.`mt4_trades`.`GROUP`
        FROM live3.`mt4_trades`
        WHERE mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and `GROUP` in (SELECT `GROUP` FROM live5.group_table 
                    WHERE LIVE = 'live3' AND COUNTRY = 'TW' and CURRENCY = 'USD')
        AND LENGTH(mt4_trades.LOGIN) > 4 AND mt4_trades.CMD < 2 
        GROUP BY LOGIN, SYMBOL, CMD       """
    return get_country_df(query_bridge_SQL)

# Get the Dataframe for CN
def get_cn_df():

    query_bridge_SQL = """SELECT LOGIN, SYMBOL, CMD, VOLUME * 0.01 as VOLUME, OPEN_TIME, SWAPS + PROFIT AS REVENUE, live1.`mt4_trades`.`GROUP`
    FROM live1.`mt4_trades`,
    (SELECT `GROUP`,COUNTRY,CURRENCY,BOOK FROM live5.group_table WHERE LIVE = 'live1' AND COUNTRY = 'CN') AS X 
    WHERE mt4_trades.`GROUP`=X.`GROUP` AND LENGTH(mt4_trades.LOGIN) > 4 AND mt4_trades.CMD < 2 
    AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00'
    """
    return get_country_df(query_bridge_SQL)


def get_country_df(sql_statement):
    sql_query = text(sql_statement)

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    #result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]

    df = pd.DataFrame(data=result_data, columns=result_col) # creating a dataframe

    # Trying to get the net Volume
    df['NET_VOLUME'] = df.apply(lambda x: x['VOLUME'] if x['CMD'] == 0 else -1 * x['VOLUME'], axis=1)

    return df


# Want to save the page log into SQL to know who has been using the pages.
@analysis.before_request
def before_request():

    # Don't want to record any ajax calls.
    endpoint = "{}".format(request.endpoint)
    if endpoint.lower().find("ajax") >=0:
        return
    else:

        # check if the user is logged.
        if not current_user.is_authenticated:
            return

        # IP Address of the client/request-er.
        # remote_addr = request.remote_addr
        # endpoint = request.endpoint
        raw_sql = "INSERT INTO aaron.Aaron_Page_History (login, IP, full_path, datetime) VALUES ('{login}', '{IP}', '{full_path}', now()) ON DUPLICATE KEY UPDATE datetime=now()"
        sql_statement = raw_sql.format(login=current_user.id,
                                       IP=request.remote_addr,
                                       full_path=request.full_path)
        # #
        # print("--------- Before request ------")
        # print(": {}".format(request))
        # print("request access_route: {}".format(request.access_route))
        # print("request base_url: {}".format(request.base_url))
        # print("request endpoint: {}".format(request.endpoint))
        # print("request full_path: {}".format(request.full_path))
        # print("request host: {}".format(request.host))
        # print("request host_url: {}".format(request.host_url))
        # print("request remote_addr: {}".format(request.remote_addr))
        # print(sql_statement)

        async_sql_insert_raw(app=current_app._get_current_object(), sql_insert=sql_statement)


@analysis.route('/Save_MT4_BGI_Float', methods=['GET', 'POST'])
@roles_required()
def save_mt4_BGI_float():

    title = "Save MT4 BGI Float"
    header = "Saving MT4 BGI Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup("<b>Saving BGI Float Data.</b><br>"+
                         "Saving it into aaron.BGI_Float_History_Save<br>" +
                         "Save it by Country, by Symbol.<br>" +
                         "Country Float and Symbol Float will be using this page.<br>"+
                         "Table time is in Server [Live 1] Timing.<br>" +
                         "Revenue has been all flipped (*-1) regardless of A or B book.<br><br>")

        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename = background_pic("save_BGI_float"),
                           icon="css/save_Filled.png", Table_name="Save MT4 Floating 💾", title=title,
                           ajax_url=url_for('analysis.save_BGI_MT4_float_Ajax', _external=True), header=header, setinterval=12,
                           description=description, replace_words=Markup(["Today"]))


# Insert into aaron.BGI_Float_History_Save
# Will select, pull it out into Python, before inserting it into the table.
# Tried doing Insert.. Select. But there was desdlock situation..
@analysis.route('/save_BGI_float_Ajax', methods=['GET', 'POST'])
@roles_required()
def save_BGI_MT4_float_Ajax(update_tool_time=1):

    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([{'Update time': "Not updating, as Market isn't opened. {}".format(Get_time_String())}])


    if check_session_live1_timing() == False:    # If False, It needs an update.

        # TOREMOVE: Comment out the print.
        print("Saving Previous Day PnL")
        #TODO: Maybe make this async?
        save_previous_day_mt4_PnL()                 # We will take this chance to get the Previous day's PnL as well.
    # else:
    #     print("BGI Save MT4 Float. No need to save previous day PnL.")

    # Want to reduce the query overheads. So try to use the saved value as much as possible.
    server_time_diff_str = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session \
                else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"

    # Call the prepared statement saved in SQL.
    sql_statement = "call `BGI_B_Book_Floating_By_Country_Symbol`()"


    # Want to get results for the above query, to get the Floating PnL
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result

    result_col = raw_result.keys() # The column names

    #end = datetime.datetime.now()
    #print("\nSaving [Pulling] floating PnL tool: {}s\n".format((end - start).total_seconds()))

    # Since the Country and Symbols are Primary keys,
    # We want to add up the sums.
    # We first need to transform the CFDs into their original Core symbol
    country_symbol = {}
    for r in result_data:
        country = r[1]
        symbol = cfd_core_symbol(r[2])        # ## Want to get core value
        net_floating_volume = r[3]
        sum_floating_volume = r[4]
        floating_revenue = r[5]
        sum_closed_volume = r[6]
        closed_profit = r[7]
        date_time = r[8]
        if (country, symbol, date_time) not in country_symbol:
            # List/tuple packing
            country_symbol[(country, symbol, date_time)] = [net_floating_volume, sum_floating_volume, floating_revenue, sum_closed_volume, closed_profit]
        else:
            country_symbol[(country, symbol, date_time)][0] += net_floating_volume
            country_symbol[(country, symbol, date_time)][1] += sum_floating_volume
            country_symbol[(country, symbol, date_time)][2] += floating_revenue
            country_symbol[(country, symbol, date_time)][3] += sum_closed_volume
            country_symbol[(country, symbol, date_time)][4] += closed_profit
    #
    # for k,d in country_symbol.items():
    #     print("{} : {}".format(k,d))

    # Put it back to the same array.
    result_data_clean = []
    for k,d in country_symbol.items():
        country = k[0]
        symbol = k[1]
        date_time = k[2]
        # List unpacking
        [net_floating_volume, sum_floating_volume, floating_revenue, sum_closed_volume, closed_profit] = d
        result_data_clean.append([country, symbol, sum_floating_volume, floating_revenue, net_floating_volume,closed_profit, sum_closed_volume, date_time])


    # Want to clean up the data
    # Date, as well as decimals to string.
    result_clean = [["'{}'".format(d) if  not isinstance(d, datetime.datetime) else "'{}'".format(Get_SQL_Timestring(d))
                         for d in r] for r in result_data_clean]
    # Form the string into (), values for the insert.
    result_array = ["({})".format(" , ".join(r)) for r in result_clean]

    # Want to insert into the Table.
    insert_into_table = text("INSERT INTO aaron.BGI_Float_History_Save (`country`, `symbol`, `floating_volume`, " +
                             "`floating_revenue`, `net_floating_volume`, `closed_revenue_today`, `closed_vol_today`," +
                             "`datetime`) VALUES {} ON DUPLICATE KEY UPDATE ".format(" , ".join(result_array)) +
                             " `floating_volume` = VALUES(`floating_volume`) , " +
                             " `floating_revenue` = VALUES(`floating_revenue`), `net_floating_volume` = VALUES(`net_floating_volume`), " +
                             " `closed_revenue_today` = VALUES(`closed_revenue_today`), `closed_vol_today` = VALUES(`closed_vol_today`)")

    #print(insert_into_table)
    raw_result = db.engine.execute(insert_into_table)  # Want to insert into the table.




    # Insert into the Checking tools.
    if (update_tool_time == 1):
        Tool = "bgi_float_mt4_history_save"
        sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
                     " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
                         Tool=Tool)
        raw_insert_result = db.engine.execute(sql_insert)

    #end = datetime.datetime.now()
    #print("\nSaving floating PnL tool: {}s\n".format((end - start).total_seconds()))
    return json.dumps([{'Update time': Get_time_String()}])



@analysis.route('/BGI_Country_Float', methods=['GET', 'POST'])
@roles_required()
def BGI_Country_Float():


    title = "Country Float"
    header = "Country Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    description = Markup("<b>Floating PnL By Country.</b><br> Revenue = Profit + Swaps<br>" +
                         "_A Groups shows Client side PnL (<span style = 'background-color: #C7E679'>Client Side</span>).<br>" +
                         "All others are BGI Side (Flipped, BGI Side)<br><br>" +
                         "Data taken from aaron.BGI_Float_History_Save table.<br><br>" +
                         "Tableau Data taken off same table." +
                         "Page will automatically refresh. However, it (<span style = 'background-color: yellow'>will stop with tab is idle</span>. (Hidden / Minimized) <br>" +
                         "Tableau Viz will auto refresh every 2 mins.")



        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Country_Float.html", backgroud_Filename=background_pic("BGI_Country_Float"), icon="css/Globe.png", Table_name="Country Float 🌎", \
                           title=title, ajax_url=url_for('analysis.BGI_Country_Float_ajax', _external=True), ajax_clear_cookie_url=url_for("analysis.Clear_session_ajax", _external=True), header=header, setinterval=15,
                           description=description, replace_words=Markup(['(Client Side)']))




# Want to check if the PnL has been saved for the previous day.
# Return False IF No PnL for previous Day
def check_previous_day_pnl_in_DB():

    return_val = False
    live1_server_difference = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in\
                                                                session else get_live1_time_difference()
    # Use live 1 time, plus 5 hours,to offset any DST
    live1_start_time = liveserver_Previousday_start_timing(live1_server_difference=live1_server_difference, hour_from_2300=5)

    pnl_date = live1_start_time.strftime("%Y-%m-%d")

    insert_into_table = text("SELECT count(*) FROM aaron.bgi_dailypnl_by_country_group WHERE DATE ='{}'".format(pnl_date))
    raw_result = db.engine.execute(insert_into_table)  # Want to insert into the table.
    result_data = raw_result.fetchall()     # Return Result
    if len(result_data) > 0:
        if len(result_data[0])> 0:
            if result_data[0][0] > 2:
                return_val = True

    return return_val




# To save previous working day PnL to aaron.bgi_dailypnl_by_country_group
def save_previous_day_mt4_PnL(force_update=False):

    # If PnL Has been saved already. We don't need to save it again.
    # Unless there's a need to force it to update.
    if check_previous_day_pnl_in_DB() and not force_update:
        print("PnL for previous day has been saved. Not saving it now.")
        return

    #TODO: Remove
    #print("Updating DB Yesterday PnL")

    # Trying to reduce the over-heads as much as possible.
    live1_server_difference = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()

    live1_start_time = liveserver_Previousday_start_timing(live1_server_difference=live1_server_difference, hour_from_2300=0)
    live5_start_time = liveserver_Previousday_start_timing(live1_server_difference=live1_server_difference,
                                                           hour_from_2300=1)
    pnl_date = (live1_start_time + datetime.timedelta(hours=5)).strftime("%Y-%m-%d")    # Use live 1 time, plus 5 hours,to offset any DST

    # Want to reduce the overheads
    ServerTimeDiff_Query = "{}".format(session["live1_sgt_time_diff"]) if "live1_sgt_time_diff" in session \
        else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"

    live123_Time_String = "mt4_trades.close_time >= '{}' and mt4_trades.close_time < '{}'".format(live1_start_time, live1_start_time + datetime.timedelta(days=1))
    live5_Time_String = "mt4_trades.close_time >= '{}' and mt4_trades.close_time < '{}'".format(live5_start_time, live5_start_time + datetime.timedelta(days=1))

    sql_statement = """SELECT COUNTRY, SYMBOL1 as SYMBOL, SUM(Closed_Vol),  -1*SUM(CLOSED_PROFIT) AS REVENUE, DATE_SUB(now(),INTERVAL ({ServerTimeDiff_Query}) HOUR) as DATETIME FROM(
    (SELECT 'live1'AS LIVE,group_table.COUNTRY, 
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
	SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS Closed_Vol,
		ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') ) THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
	
	FROM live1.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND {live123_Time_String} AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CMD <2 AND group_table.LIVE = 'live1' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    UNION
    (SELECT 'live2' AS LIVE,group_table.COUNTRY, 
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS Closed_Vol,
		ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') ) THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
    FROM live2.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND {live123_Time_String} AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CMD <2 AND group_table.LIVE = 'live2' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    UNION
    (SELECT 'live3' AS LIVE,group_table.COUNTRY, 
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS Closed_Vol,
     ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') ) THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
    FROM live3.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND {live123_Time_String}  AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM live3.cambodia_exclude)


    AND mt4_trades.CMD <2 AND group_table.LIVE = 'live3' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    UNION
    (SELECT 'live5' AS LIVE,group_table.COUNTRY,  
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS Closed_Vol,
    ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') ) THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') ) THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
    FROM live5.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND {live5_Time_String} AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CMD <2 AND group_table.LIVE = 'live5' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    ) AS B 
    GROUP BY B.COUNTRY, B.SYMBOL1
    HAVING COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST')
    ORDER BY COUNTRY, SYMBOL""".format(ServerTimeDiff_Query=ServerTimeDiff_Query, live123_Time_String=live123_Time_String,live5_Time_String=live5_Time_String)


    #print(sql_query)

    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result

    result_col = raw_result.keys() # The column names


    # Since the Country and Symbols are Primary keys,
    # We want to add up the sums.
    country_symbol = {}
    for r in result_data:
        country = r[0]
        # Want to get core value
        symbol = cfd_core_symbol(r[1])
        close_vol = r[2]
        revenue = r[3]
        date_time = r[4]

        if (country, symbol, date_time) not in country_symbol:
            # List/tuple packing
            country_symbol[(country, symbol, date_time)] = [close_vol, revenue]
        else:
            country_symbol[(country, symbol, date_time)][0] += close_vol
            country_symbol[(country, symbol, date_time)][1] += revenue

    # Put it back to the same array.
    result_data_clean = []
    for k,d in country_symbol.items():
        country = k[0]
        symbol = k[1]
        date_time = k[2]
        # List unpacking
        [close_vol, revenue] = d
        result_data_clean.append([pnl_date, country, symbol, close_vol, revenue, date_time])

    # Want to clean up the data
    # Date, as well as decimals to string.
    result_clean = [["'{}'".format(d) if  not isinstance(d, datetime.datetime) else "'{}'".format(Get_SQL_Timestring(d))
                         for d in r] for r in result_data_clean]
    # Form the string into (), values for the insert.
    result_array = ["({})".format(" , ".join(r)) for r in result_clean]

    # Want to insert into the Table.
    insert_into_table = text("INSERT INTO aaron.bgi_dailypnl_by_country_group (`date`, `country`, `symbol`, `volume`, " +
                             "`revenue`, `update_time`) VALUES {} ON DUPLICATE KEY UPDATE VOLUME=VALUES(VOLUME), REVENUE=VALUES(REVENUE), UPDATE_TIME=VALUES(UPDATE_TIME)".format(" , ".join(result_array)))
    raw_result = db.engine.execute(insert_into_table)  # Want to insert into the table.

    return


# Query SQL to return the previous day's PnL By Country
def get_country_daily_pnl():

    # Want to check what is the date that we should be retrieving.
    # Trying to reduce the over-heads as much as possible.
    live1_server_difference = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()

    # Want the start date. Need to add a few hours after 2300 to increase the date by 1.
    live1_start_time = liveserver_Previousday_start_timing(live1_server_difference=live1_server_difference, hour_from_2300=5)
    date_of_pnl = "{}".format(live1_start_time.strftime("%Y-%m-%d"))

    sql_statement = """SELECT COUNTRY, SUM(VOLUME) AS VOLUME, SUM(REVENUE) AS REVENUE, DATE
            FROM aaron.`bgi_dailypnl_by_country_group`
            WHERE DATE = '{}'
            GROUP BY COUNTRY""".format(date_of_pnl)

    # Want to get results for the above query, to get the Floating PnL
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()  # The column names

    if len(result_data) <= 0:   # If there are no data.
        # Force it to update yesterday's PnL, if it wasn't..
        save_previous_day_mt4_PnL(force_update=True)

    # If empty, we just want to return an empty data frame. So that the following merge will not cause any issues
    return_df = pd.DataFrame(result_data, columns=result_col) if len(result_data) > 0 else pd.DataFrame()
    return return_df



# Clear session data.
# Called when refreshing cookies.
@analysis.route('/Clear_session_ajax', methods=['GET', 'POST'])
@roles_required()
def Clear_session_ajax(flash_details=False):

    list_of_pop = clear_session_cookies()

    if flash_details:   # If we need to flash the details out.
        # Flash the details, but want to return to the main app page.
        flash("Session Cleared: {}".format(", ".join(list_of_pop)))

    return redirect(url_for("main_app.index"))



@analysis.route('/Clear_session', methods=['GET', 'POST'])
@roles_required()
def Clear_session():

    return redirect(url_for("analysis.Clear_session_ajax", flash_details=True))



# Get BGI Float by country
@analysis.route('/BGI_Country_Float_ajax', methods=['GET', 'POST'])
def BGI_Country_Float_ajax():

    #start = datetime.datetime.now()
    # print(current_app.permanent_session_lifetime)
    # print(session)

    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    # Will check the timing
    # Need to ensure that there is something there, in case of a race condition where the PnL is still saving but it's been queried.
    if check_session_live1_timing() == True and \
            "yesterday_pnl_by_country" in session:
        #print("From in memory")
        # From in memory of session
        #print("live1_sgt_time_update ' {}".format(session["live1_sgt_time_update"]))
        #print(len(session["yesterday_pnl_by_country"]))
        df_yesterday_country_float = pd.DataFrame.from_dict(session["yesterday_pnl_by_country"])
        #print(df_yesterday_country_float)
    else:       # If session timing is outdated, or needs to be updated.
        #print("Getting from DB")

        df_yesterday_country_float = get_country_daily_pnl()
        if "DATE" in df_yesterday_country_float:  # We want to save it as a string.
            #print("DATE IN")
            df_yesterday_country_float['DATE'] = df_yesterday_country_float['DATE'].apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime.date) else x)
        session["yesterday_pnl_by_country"] =  df_yesterday_country_float.to_dict()


    #print(session["yesterday_pnl_by_country"])
    # if "DATE" in df_yesterday_country_float:    # We want to save it as a string.
    #     print("DATE IN")
    #     df_yesterday_country_float['DATE'] = df_yesterday_country_float['DATE'].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime.date) else x)

        #print(df_yesterday_country_float['DATE'])
        #print(df_yesterday_country_float['DATE'].apply(type))

    #print(dict(zip(["{}".format(c) for c in list(df_yesterday_country_float.columns) if c.find("COUNTRY") == -1], \
    #    ["YESTERDAY_{}".format(c) for c in list(df_yesterday_country_float.columns) if c.find("COUNTRY") == -1])))

    # Want to change the names of the columns. But we need to preserve the COUNTRY name since we use that to merge.
    df_yesterday_country_float.rename(columns=dict(zip(["{}".format(c) for c in list(df_yesterday_country_float.columns) if c.find("COUNTRY") == -1], \
        ["YESTERDAY_{}".format(c) for c in list(df_yesterday_country_float.columns) if c.find("COUNTRY") == -1])), inplace=True)

    #print(df_yesterday_country_float)
    #df_yesterday_country_float.rename(columns={"REVENUE":"YESTERDAY_REVENUE"})


    # Want to reduce the overheads.
    server_time_diff_str = " {} ".format(session["live1_sgt_time_diff"]) if "live1_sgt_time_diff" in session else \
            "SELECT RESULT FROM aaron.`aaron_misc_data` where item = 'live1_time_diff'"

    sql_statement = """SELECT COUNTRY, SUM(ABS(floating_volume)) AS FLOAT_VOLUME, SUM(floating_revenue) AS FLOAT_REVENUE,
                             SUM(CLOSED_VOL_TODAY) AS CLOSED_VOL ,SUM(CLOSED_REVENUE_TODAY) AS CLOSED_REVENUE,
                            DATE_ADD(DATETIME,INTERVAL ({ServerTimeDiff_Query}) HOUR) AS DATETIME
            FROM aaron.BGI_Float_History_Save
            WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.BGI_Float_History_Save)
            GROUP BY COUNTRY
            """.format(ServerTimeDiff_Query=server_time_diff_str)

    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()  # Column names

    df = pd.DataFrame(result_data, columns=result_col)


    # We want to show Client Side for Dealing as well as A book Clients.
    to_flip_groups = ["_A", "Dealing"]
    df['FLOAT_REVENUE'] = df.apply(lambda x: -1*x['FLOAT_REVENUE']  \
                                if any([x["COUNTRY"].find(c) >=0 for c in to_flip_groups])  \
                                else x['FLOAT_REVENUE'], axis=1)

    # For the display of table, we only want the latest data inout.
    df_to_table = df[df['DATETIME'] == df['DATETIME'].max()].drop_duplicates()

    # Join the DF by 'COUNTRY'
    if "COUNTRY" in list(df_yesterday_country_float.columns):
        df_to_table = df_to_table.merge(df_yesterday_country_float, on="COUNTRY", how='left')

    df_to_table.fillna(0, inplace=True) # Want to fill up all the empty ones with 0

    # Get Datetime into string
    df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x))

    # Sort by Revenue
    df_to_table.sort_values(by=["FLOAT_REVENUE"], inplace=True, ascending=False)


    # Want to show on the chart. Do not need additional info as text it would be too long.
    # chart_Country = list(df_to_table['COUNTRY'].values)


    # # # Want to add colors to the words.
    # Want to color the REVENUE
    cols = ["FLOAT_REVENUE" , "CLOSED_REVENUE", "YESTERDAY_REVENUE",]
    for c in cols:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(
                lambda x: """{c}""".format(c=profit_red_green(x) if isfloat(x) else x))

    # Don't want the zeros. 0 is discarded.
    datetime_pull =  [c for c in list(df_to_table['DATETIME'].unique()) if c != 0] if  "DATETIME" in df_to_table else  ["No Datetime in df."]

    # Get the date. Don't want to waste space showing it in the columns.
    yesterday_datetime_pull = [c for c in list(df_to_table['YESTERDAY_DATE'].unique()) if c != 0] if "YESTERDAY_DATE" in df_to_table else ["No YESTERDAY_DATE in df."]

    # If we need to rename the columns. We need to change here as well! WE removed "DATETIME", "YESTERDAY_DATE",
    show_col = ["COUNTRY", "FLOAT_VOLUME", "FLOAT_REVENUE" ,"CLOSED_VOL", "CLOSED_REVENUE", "YESTERDAY_REVENUE", "YESTERDAY_VOLUME"]
    df_show_col = [d for d in show_col if d in df_to_table]


    # Reduce the number of columns.
    df_to_table = df_to_table[df_show_col]

    # Want to hyperlink the FLOAT Volume as well.
    df_to_table['FLOAT_VOLUME'] = df_to_table.apply(lambda x: Country_Trades_url(x['COUNTRY'], "{:,.2f}".format(x['FLOAT_VOLUME'])), axis=1)
    # Want to hyperlink the CLOSED_VOL as well.
    df_to_table['CLOSED_VOL'] = df_to_table.apply(lambda x: Country_Trades_url(x['COUNTRY'], "{:,.2f}".format(x['CLOSED_VOL'])), axis=1)
    # Want to append the Country URL 
    df_to_table['COUNTRY'] = df_to_table['COUNTRY'].apply(Country_Trades_url)

    # Adding words to the country name, to be flagged out by Javascript to color the cell
    df_to_table['COUNTRY'] = df_to_table.apply(lambda x: '{} (Client Side)'.format(x['COUNTRY']) if (
                x["COUNTRY"].find("_A") > 0 or x["COUNTRY"].find('Dealing') >= 0) else x['COUNTRY'], axis=1)



    df_records = df_to_table.to_records(index=False)
    dataframe_col = list(df_to_table.columns)
    df_records = [list(a) for a in df_records]

    #print(emoji.emojize('Python is :china: :TW: :NZ: :HK:'))

    # Want to clean up the data
    # result_clean = [[Get_time_String(d) if isinstance(d, datetime.datetime) else d for d in r] for r in result_data]

    return_val = [dict(zip(dataframe_col,d)) for d in df_records]
    #print(return_val)

    #end = datetime.datetime.now()
    #print("\nGetting Country PnL tool[Before Chart]: {}s\n".format((end - start).total_seconds()))

    #fig = []
    # For plotting.
    # bar_color = df_to_table['FLOAT_REVENUE'].apply(lambda x: "green" if x >= 0 else 'red')
    # fig = go.Figure(data=[
    #     go.Bar(name="Total Volume", y=df_to_table['FLOAT_REVENUE'], x=chart_Country, text=df_to_table['FLOAT_REVENUE'], textposition='outside',
    #            cliponaxis=False, textfont=dict(size=14), marker_color=bar_color)
    # ])

    # fig.update_layout(
    #     autosize=True,
    #     margin=dict(pad=1),
    #     yaxis=dict(
    #         title_text="Floating Revenue",
    #         ticks="outside", tickcolor='white', ticklen=15,
    #         layer='below traces'
    #     ),
    #     yaxis_tickfont_size=14,
    #     xaxis=dict(
    #         title_text="Country"
    #     ),
    #     xaxis_tickfont_size=15,
    #     title_text='Floating Revenue by Country',
    #     titlefont=dict(size=28, family="'Montserrat', sans-serif"),
    #     title_x=0.5
    # )

    #end = datetime.datetime.now()
    #print("\nGetting Country PnL tool: {}s\n".format((end - start).total_seconds()))

    return json.dumps([return_val, ", ".join(datetime_pull), ", ".join(yesterday_datetime_pull)], cls=plotly.utils.PlotlyJSONEncoder)
    #return json.dumps([return_val], cls=plotly.utils.PlotlyJSONEncoder)



# Force an error in Flask
@analysis.route('/Error_ajax', methods=['GET', 'POST'])
def Error_ajax():


    sql_statement = """SELECT * FROM aaron.Table_dosn't_exist"""
    sql_query = text(sql_statement)

    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()  # Column names

    return json.dumps([{"return":"Should have sent an email for error in Flask"}])



@analysis.route('/BGI_Symbol_Float', methods=['GET', 'POST'])
@roles_required()
def BGI_Symbol_Float():

    title = "MT4 Symbol Float"
    header = "MT4 Symbol Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup("<b>Floating PnL By Symbol.</b><br> Revenue = Profit + Swaps<br>"+
                         "B book Groups Only.<br>"+
                         'Using Live5.group_table where book = "B"<br>' +
                         'HK Is excluded from all symbols <br>'  +
                         '<br><br>' +

                         'NET LOTS : Net Lots BGI is holding. (Buy +ve, Sell -ve)<br>' +
                         'FLOATING LOTS : Total Open Lots (Regardless of Buy or sell)<br>' +
                         'REVENUE : Floating USD Converted Profit + Swaps. (BGI SIde)<br>' +
                         'TODAY LOTS : Lots closed today.<br>' +
                         'TODAY REVENUE : Closed Revenue for today<br>' +
                         'YESTERDAY LOTS : Total Lots closed in the last trading day<br>' +
                         'YESTERDAY REVENUE : REVENUE of all closed trades in the last trading day<br>' +
                         '<br><br>' +
                         'Values are all on <b>BGI Side</b>. <br>' +
                         'Sort by absolute net volume.<br>'+
                         "Yesterday Data saved in cookies.<br>" +
                         "Taking Live prices from Live 1 q Symbols")




        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename=background_pic("BGI_Symbol_Float"),
                           icon= "",
                           Table_name="MT4 Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('analysis.BGI_Symbol_Float_ajax', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(Client Side)']))



# Get BGI Float by Symbol
@analysis.route('/BGI_Symbol_Float_ajax', methods=['GET', 'POST'])
@roles_required()
def BGI_Symbol_Float_ajax():


    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    if check_session_live1_timing() == True and "yesterday_mt4_pnl_by_symbol" in session \
            and len(session["yesterday_mt4_pnl_by_symbol"]) > 0:
        # From "in memory" of session
        # print(session)
        df_yesterday_symbol_pnl = pd.DataFrame.from_dict(session["yesterday_mt4_pnl_by_symbol"])
    else:  # If session timing is outdated, or needs to be updated.

        #print("Getting yesterday symbol PnL from DB")
        df_yesterday_symbol_pnl = get_mt4_symbol_daily_pnl()
        #print(df_yesterday_symbol_pnl)
        if "DATE" in df_yesterday_symbol_pnl:  # We want to save it as a string.
            # print("DATE IN")
            df_yesterday_symbol_pnl['DATE'] = df_yesterday_symbol_pnl['DATE'].apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime.date) else x)

        session["yesterday_mt4_pnl_by_symbol"] =  df_yesterday_symbol_pnl.to_dict()

    # Want to get the Unique date for the "Yesterday" date.
    # Want to know the date of the symbol "yesterday" details.
    yesterday_datetime_pull = [c for c in list(df_yesterday_symbol_pnl['DATE'].unique()) if
                               c != 0] if "DATE" in df_yesterday_symbol_pnl else ["No YESTERDAY_DATE in df."]

    # We already know the date. No need to carry on with this data.

    if "DATE" in df_yesterday_symbol_pnl:
        df_yesterday_symbol_pnl.pop('DATE')

    # Want to change the names of the columns. But we need to preserve the COUNTRY name since we use that to merge.
    df_yesterday_symbol_pnl.rename(columns=dict(zip(["{}".format(c) for c in list(df_yesterday_symbol_pnl.columns) if c.find("SYMBOL") == -1], \
        ["YESTERDAY_{}".format(c) for c in list(df_yesterday_symbol_pnl.columns) if c.find("SYMBOL") == -1])), inplace=True)




    #print(df_yesterday_symbol_pnl)

    server_time_diff_str = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session \
                else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"


    sql_statement = """SELECT aaron.BGI_Float_History_Save.SYMBOL, SUM(ABS(floating_volume)) AS VOLUME, -1 * SUM(net_floating_volume) AS NETVOL, 
                    SUM(floating_revenue) AS REVENUE, 
                    SUM(closed_vol_today) as "TODAY_VOL",
                    SUM(closed_revenue_today) as "TODAY_REVENUE",                            
                                                P.ASK, P.BID,
                                                DATE_ADD(DATETIME,INTERVAL ({ServerTimeDiff_Query}) HOUR) AS DATETIME
    FROM aaron.BGI_Float_History_Save 
                    LEFT JOIN live1.mt4_prices as P ON  CONCAT(aaron.BGI_Float_History_Save.SYMBOL, "q") = P.SYMBOL
    WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.BGI_Float_History_Save)
    AND COUNTRY IN (SELECT COUNTRY from live5.group_table where BOOK = "B") 
    AND COUNTRY not like "HK"
    GROUP BY SYMBOL
    ORDER BY REVENUE DESC""".format(ServerTimeDiff_Query=server_time_diff_str)

    #print(sql_statement)
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result

    result_col = raw_result.keys()  # Column names



    #end = datetime.datetime.now()
    #print("\nGetting SYMBOL PnL tool[After Query]: {}s\n".format((end - start).total_seconds()))

    df = pd.DataFrame(result_data, columns=result_col)

    # For the display of table, we only want the latest data inout.
    df_to_table = df[df['DATETIME'] == df['DATETIME'].max()].drop_duplicates()

    # Get Datetime into string
    df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x))


    datetime_pull =  [c for c in list(df_to_table['DATETIME'].unique()) if c != 0] if  "DATETIME" in df_to_table else  ["No Datetime in df."]



    # Sort by abs net volume
    df_to_table["ABS_NET"] = df_to_table["NETVOL"].apply(lambda x: abs(x))
    df_to_table.sort_values(by=["ABS_NET"], inplace=True, ascending=False)
    df_to_table.pop('ABS_NET')


    # We already know the date. No need to carry on with this data.
    if "DATETIME" in df_to_table:
        df_to_table.pop('DATETIME')


    # SQL sometimes return values with lots of decimal points.
    # We only want to show afew. Else, it takes up too much screen spaace.
    if "BID" in df_to_table:
        df_to_table["BID"] = df_to_table["BID"].apply(lambda x: "{:2.5f}".format(x) if (isfloat(decimal.Decimal(str(x)).as_tuple().exponent)
                                                                                        and (decimal.Decimal(str(x)).as_tuple().exponent < -5)) else x)

    # SQL sometimes return values with lots of decimal points.
    # We only want to show afew. Else, it takes up too much screen spaace.
    if "ASK" in df_to_table:
        df_to_table["ASK"] = df_to_table["ASK"].apply(lambda x: "{:2.5f}".format(x) if (isfloat(decimal.Decimal(str(x)).as_tuple().exponent)
                                                                                        and (decimal.Decimal(str(x)).as_tuple().exponent < -5)) else x)

    # Go ahead to merge the tables, and add the hyperlink
    if "SYMBOL" in df_to_table and "SYMBOL" in df_yesterday_symbol_pnl:
        df_to_table = df_to_table.merge(df_yesterday_symbol_pnl, on="SYMBOL", how='left')
        df_to_table.fillna("-", inplace=True)  # Want to fill up all the empty ones with -
        # Want to hyperlink Yesterday Revenue. To show yesterday's date.
        # Add comma if it's a float.

        # Hyperlink this.
        if "YESTERDAY_REVENUE" in df_to_table:
            #df_to_table["YESTERDAY_REVENUE"] = df_to_table["YESTERDAY_REVENUE"].apply(lambda x: "{:,.2f}".format(x) if isfloat(x) else x)
            # Hyperlink it.
            df_to_table["YESTERDAY_REVENUE"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_REVENUE}</a>""".format( \
                                                                url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                                YESTERDAY_REVENUE=profit_red_green(x["YESTERDAY_REVENUE"]) if isfloat(x["YESTERDAY_REVENUE"]) else x["YESTERDAY_REVENUE"] ),
                                                                axis=1)
        # Also want to hyperlink this.
        # Just.. to have more hypterlink. HA ha ha.
        # Haven changed name yet. So it's still names "volume"
        if "YESTERDAY_VOLUME" in df_to_table:
            # Hyperlink it.
            df_to_table["YESTERDAY_VOLUME"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_VOLUME}</a>""".format( \
                                                                url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                                YESTERDAY_VOLUME=x["YESTERDAY_VOLUME"]),
                                                                axis=1)

    # Need to check if the columns are in the df.
    # taking this chance to re-arrange them as well.
    # col_of_df = [c for c in ["SYMBOL", "NETVOL", "VOLUME", "REVENUE", "TODAY_VOL", "TODAY_REVENUE", "BID", "ASK","YESTERDAY_VOLUME", "YESTERDAY_REVENUE"] if c in  list(df_to_table.columns)]

    #df_records = df_to_table[col_of_df].to_records(index=False)
    #df_records = [list(a) for a in df_records]
    #return_val = [dict(zip(col_of_df,d)) for d in df_records]


    # Want to hyperlink it.
    df_to_table["SYMBOL"] = df_to_table["SYMBOL"].apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{symbol}</a>'.format(symbol=x,
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x, book="b")))

   # # # Want to add colors to the words.
    # Want to color the REVENUE
    cols = ["REVENUE", "TODAY_REVENUE", "NETVOL"]
    for c in cols:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: """{c}""".format(c=profit_red_green(x) if isfloat(x) else x))



    #Rename the VOLUME to LOTs
    df_to_table.rename(columns={"NETVOL": "NET_LOTS", "VOLUME": "FLOATING_LOTS",
                                "TODAY_VOL" : "TODAY_LOTS", "YESTERDAY_VOLUME": "YESTERDAY_LOTS"}, inplace=True)

    ## Want to check if YESTERDAY_VOLUME and YESTERDAY_REVENUE are in.
    # # Ment for debugging the 5.10am to 7.55am issue.
    if 'YESTERDAY_LOTS' not in df_to_table.columns or 'YESTERDAY_REVENUE' not in df_to_table.columns:
        # Send email
        #print(session)
        session_array = []
        for u in list(session.keys()):
            session_array.append("{} : {}".format(u, session[u]))
        #print("<br><br>".join(session_array))
        # async_send_email(To_recipients=["aaron.lim@blackwellglobal.com"], cc_recipients=[], Subject="Yesterday_lots or Yesterday_revenue Missing",
        #                  HTML_Text="df_to_table <br><br>{}<br><br>session<br><br>{}".format(df_to_table.to_html() , "<br><br>".join(session_array)),
        #                  Attachment_Name=[])


    # Want only those columns that are in the df
    # Might be missing cause the PnL could still be saving.
    col_of_df_return = [c for c in ["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE", "TODAY_LOTS", \
                                    "TODAY_REVENUE", "BID", "ASK","YESTERDAY_LOTS", "YESTERDAY_REVENUE"] \
                        if c in  list(df_to_table.columns)]

    # Pandas return list of dicts.
    return_val = df_to_table[col_of_df_return].to_dict("record")



    #end = datetime.datetime.now()
    #print("\nGetting Country PnL tool[Before Chart]: {}s\n".format((end - start).total_seconds()))

    # For plotting.

    fig = []
    # bar_color = df_to_table['REVENUE'].apply(lambda x: "green" if x >= 0 else 'red')
    # fig = go.Figure(data=[
    #     go.Bar(name="Total Volume", y=df_to_table['REVENUE'], x=chart_Country, text=df_to_table['REVENUE'], textposition='outside',
    #            cliponaxis=False, textfont=dict(size=14), marker_color=bar_color)
    # ])
    #
    # fig.update_layout(
    #     autosize=True,
    #     margin=dict(pad=1),
    #     yaxis=dict(
    #         title_text="Revenue",
    #         ticks="outside", tickcolor='white', ticklen=15,
    #         layer='below traces'
    #     ),
    #     yaxis_tickfont_size=14,
    #     xaxis=dict(
    #         title_text="Country"
    #     ),
    #     xaxis_tickfont_size=15,
    #     title_text='Floating Revenue by Country',
    #     titlefont=dict(size=28, family="'Montserrat', sans-serif"),
    #     title_x=0.5
    # )

    #end = datetime.datetime.now()
    #print("\nGetting SYMBOL PnL tool: {}s\n".format((end - start).total_seconds()))
    return json.dumps([return_val, ", ".join(datetime_pull), ", ".join(yesterday_datetime_pull)], cls=plotly.utils.PlotlyJSONEncoder)

# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@analysis.route('/BGI_Symbol_Float/Open_Symbol/<book>/<symbol>', methods=['GET', 'POST'])
@roles_required()
def symbol_float_trades(symbol="", book="b"):

    title = "{} ({})".format(symbol, book.upper())
    header = "{} Floating Trades".format(symbol)

    if book.lower() == "b":
        header += "(B 📘)"
    elif book.lower() == "a":
        header += "(A 📕)"

    table_ledgend = "COUNTRY: Country that Client Group is in.<br>" + \
                    "GROUP: Client Group.<br>" + \
                    "LOTS : Lots of trades (Or total sum, where applies).<br>" + \
                    "NET LOTS : Cross tally of buy (+ve) and sell (-ve).<br>" + \
                    "CONVERTED REVENUE : SWAPS + PROFIT converted to USD.<br>" + \
                    "REBATE : Amount (Sum) of rebate paid out.<br>" + \
                    "SWAPS : Amount(Sum) of swaps for trades.<br>" + \
                    "PROFIT : PnL (Sum) for trades.<br>" + \
                    "TOTAL PROFIT: CONVERTED REVENUE - REBATE. This is how much BGI Earns.<br>" + \
                    "<br><br>" + \
                    "REBATE will be highlighted if REVENUE is -ve, But REVENUE + REBATE >= 0.<br>" +\
                    "That's to say, Client is still profitable."

    description = Markup("Showing Open trades for {}<br>Details are on Client side.<br><br>{}".format(symbol, table_ledgend))

    if symbol == "" :  # There are no information.
        flash("There were no symbol details.")
        return redirect(url_for("main_app.index"))

    # Table names will need be in a dict, identifying if the table should be horizontal or vertical.
    # Will try to do smaller vertical table to put 2 or 3 tables in a row.

    #

    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbol_float_trades"), icon="",
                           Table_name={ "Country MT4 Float (BGI Side)": "Hs5",
                                        "Country MT5 Float (BGI Side)": "Hs7",
                                        "Winning Floating Groups (Client Side)": "Hs1",
                                        "Losing Floating Groups (Client Side)": "Hs2",
                                        "Winning Floating Accounts (Client Side)": "H1",
                                        "Losing Floating Accounts (Client Side)": "H2",
                                        "Largest Lots Floating Accounts (Client Side)": "H5",
                                        "Total Volume Snapshot" : "P1",
                                        "Open Time vs Lots": "P2",
                                        "Line": "Hr1",
                                        "Winning Realised Accounts Today (Client Side)": "H3",
                                        "Losing Realised Accounts Today (Client Side)": "H4",
                                        "Largest Lots Realised Accounts Today (Client Side)": "H6",
                                        "Winning Realised Group Today (Client Side)": "Hs3",
                                        "Losing Realised Group Today (Client Side)": "Hs4",
                                        "History Daily Closed Vol": "P3",
                                        "History Daily Revenue": "P4",
                                        "Total Closed Today (BGI Side)": "V2",
                                        "Country Closed (BGI Side)": "Hs6",
                                        "Line2": "Hr2",
                                        },
                           title=title,
                           ajax_url=url_for('analysis.symbol_float_trades_ajax', _external=True, symbol=symbol, book=book, entity="none"),
                           book = book.upper(),
                           header=header, symbol=symbol,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"])) #setinterval=60,

# The Ajax call for the symbols we want to query. B Book.
@analysis.route('/Open_Symbol/<book>/symbol_float_trades_ajax/<symbol>/<entity>', methods=['GET', 'POST'])
@roles_required()
def symbol_float_trades_ajax(symbol="", book="b", entity="none"):

    start_time = datetime.datetime.now() # Want to get the datetime when this request was called.
    # Want to show 2 days back if the hour is less than 10am (SGT), else, shows 1 day only.
    opentime_day_backwards_count = 3 if datetime.datetime.now().hour < 12 else 2  # Not too many days, else it will be too small.
    opentime_day_backwards = "{}".format(get_working_day_date(datetime.date.today(), -1 * opentime_day_backwards_count))


    #print("symbol_float_trades_ajax Running.. ")

    # TODO: Solve this.
    entities = "" if entity.lower() == "none" else [entity] # Want to make it into a list.
    symbol = "" if symbol.lower() == "none" else symbol

    # If country, we need to manually check if it's a book or B book.
    if book == "none":
        book = "a" if entity.lower().find("_a") >= 1 else "b"

    print("book: {}".format(book))


    all_open_trades_start = datetime.datetime.now()
    print("entities : {}".format(entities))

    # We want to show the MT5 Position on the top left table. For now...
    country_mt5_res = {}
    if symbol != None:
        #
        # # Want to add in MT5 Details here.
        # mt5_symbol_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="""call yudi.`getFloatingTradesBySymbol`('{}', '{}')""".format(symbol, book),
        #                                                       unsync_app=current_app._get_current_object())
        #
        # mt5_symbol_results = mt5_symbol_unsync.result()
        # df_mt5_single_symbol = pd.DataFrame(mt5_symbol_results)
        # # print(df_mt5_country)
        #
        # # # All the columns
        # # ['Book', 'Live', 'PositionID', 'Login', 'Group', 'Symbol', 'BaseSymbol', 'Action', 'TimeCreate', 'TimeUpdate',
        # #  'PriceOpen', 'PriceCurrent', 'PriceSL', 'PriceTP', 'Volume', 'Profit', 'Storage', 'Country']
        #
        #
        # # BGI Side only.
        # df_mt5_country_bgi_side = df_mt5_single_symbol.copy()
        #
        # # Want to rename the columns.
        # df_mt5_country_bgi_side.rename(columns={"Volume" : "LOTS",
        #                                "Country" : "COUNTRY",
        #                                "Profit" : "PROFIT",
        #                                "Storage" : "SWAP",
        #                                'Rebate':'REBATE'}, inplace=True)
        #
        # if all([c in df_mt5_country_bgi_side for c in ["PROFIT", "SWAP"]]):
        #     df_mt5_country_bgi_side["PROFIT"] = -1 * df_mt5_country_bgi_side["PROFIT"]  # Convert to BGI Side
        #     df_mt5_country_bgi_side["SWAP"] = -1 * df_mt5_country_bgi_side["SWAP"]  # Convert to BGI Side
        #     # df_mt5_country_bgi_side["REBATE"] = -1 * df_mt5_country_bgi_side["REBATE"]  # Convert to BGI Side
        #     # df_mt5_country_bgi_side["REBATE"] = -1 * df_mt5_country_bgi_side["REBATE"]  # Convert to BGI Side
        #
        #
        # mt5_symbol_cols = ['COUNTRY', 'LOTS', 'NET_LOTS',  'PROFIT', 'REBATE', 'CONVERTED_REVENUE']
        #
        #
        #
        # # To calculate the net lots
        # if all([c in df_mt5_country_bgi_side.columns for c in ['LOTS', 'Action']]):
        #     df_mt5_country_bgi_side['NET_LOTS'] = df_mt5_country_bgi_side.apply(lambda x: x['LOTS'] if x['Action']==0 else -1*x['LOTS'], axis=1)
        #
        # # To calculate the total profit.
        # if all([c in df_mt5_country_bgi_side.columns for c in ['SWAP', 'PROFIT']]):
        #     df_mt5_country_bgi_side['CONVERTED_REVENUE'] = df_mt5_country_bgi_side['SWAP'] + df_mt5_country_bgi_side['PROFIT']
        #
        #
        # #print(df_mt5_country)
        #
        # # Create Empty DataFrame so that we can use that to append.
        # #df_mt5_single_symbol_group = pd.DataFrame()
        #
        # df_mt5_group_winning = pd.DataFrame()
        # df_mt5_group_losing = pd.DataFrame()
        #
        # if len(df_mt5_country_bgi_side) == 0: # If there are no MT5 Trades.
        #     country_mt5_res = [{f"MT5 {symbol} Floating": f"No Open trades for {symbol}"}]
        # else:           # If there are MT5 Trades.
        #     df_mt5_country_group = df_mt5_country_bgi_side[[c for c in mt5_symbol_cols if c in df_mt5_country_bgi_side.columns]]
        #     df_mt5_country_group = df_mt5_country_group.groupby("COUNTRY").sum().reset_index()
        #
        #     # Color the Profits
        #     df_mt5_country_group["CONVERTED_REVENUE"] = df_mt5_country_group["CONVERTED_REVENUE"].apply(profit_red_green)
        #     df_mt5_country_group["PROFIT"] = df_mt5_country_group["PROFIT"].apply(profit_red_green)
        #
        #     ## If there are trades on MT5, we'd like to know who are making those trades as well.
        #     ## Getting trades from MT5 Database for the individual MT5 Symbol.
        #
        #     # mt5_single_symbol_query = """call yudi.getFloatingTradesBySymbol("{}", "{}")""".format(symbol, book)
        #     # mt5_single_symbol_unsync = mt5_Query_SQL_mt5_db_engine_query(
        #     #     SQL_Query=mt5_single_symbol_query, unsync_app=current_app._get_current_object())
        #     # mt5_single_symbol = mt5_single_symbol_unsync.result()
        #
        #
        #     # df_mt5_single_symbol = pd.DataFrame(mt5_single_symbol)
        #     #print(df_mt5_single_symbol)
        #
        #     # Want to rename some of the columns.
        #     df_mt5_single_symbol.rename(columns={"Volume": "LOTS", "Group" : "GROUP", "Country" : "COUNTRY", "Profit" : "CONVERTED_REVENUE", "Storage" : "STORAGE"}, inplace=True)
        #
        #     # Want to get rid of all pre-fixes for MT5 Groupings.
        #     if "GROUP" in df_mt5_single_symbol:
        #         df_mt5_single_symbol["GROUP"] =  df_mt5_single_symbol["GROUP"].apply(lambda x: x.split("\\")[-1])
        #
        #     # Need to calculate the NET volume.
        #     if "LOTS" in df_mt5_single_symbol:
        #         df_mt5_single_symbol["NET_LOTS"] = df_mt5_single_symbol.apply(lambda x: x["LOTS"] if x["Action"]==0 else -1 * x["LOTS"] , axis=1)
        #     #print(df_mt5_single_symbol)
        #
        #     # Group the groups together.
        #     if "GROUP" in df_mt5_single_symbol:
        #         df_mt5_single_symbol_group = df_mt5_single_symbol[["COUNTRY", "GROUP", "LOTS", "NET_LOTS", "CONVERTED_REVENUE"]].groupby(["COUNTRY", "GROUP"]).sum().reset_index()
        #
        #         df_mt5_single_symbol_group.insert(0, 'Server', 'MT5')  # Want to add this column to the front of the df
        #
        #         # Split the Winning and losing.
        #         df_mt5_group_winning = df_mt5_single_symbol_group[df_mt5_single_symbol_group["CONVERTED_REVENUE"] >= 0 ]
        #         df_mt5_group_losing = df_mt5_single_symbol_group[df_mt5_single_symbol_group["CONVERTED_REVENUE"] < 0 ]
        #
        #         #print(df_mt5_single_symbol_group)
        #
        #     country_mt5_res = df_mt5_country_group.to_dict("records")

        df_mt5_country_group, df_mt5_group_winning, df_mt5_group_losing, df_mt5_login_winning, df_mt5_login_losing, \
            df_mt5_login_largestlots = mt5_symbol_individual(symbol, book)

        country_mt5_res = df_mt5_country_group.to_dict("records")

    else:
        country_mt5_res = [{f"MT5 {symbol} Floating": f"No Open trades for {symbol}"}]


    # Want to reduce the overheads
    # We can't do this in the threads.
    ServerTimeDiff_Query = "{}".format(session["live1_sgt_time_diff"]) if "live1_sgt_time_diff" in session \
        else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"

    all_trades_unsync = symbol_all_open_trades(app=current_app._get_current_object(),
                                               ServerTimeDiff_Query=ServerTimeDiff_Query, symbol=symbol,
                                               book=book, entities=entities)

    # Snap shot of the volume.
    # Need to pass in the app as this would be using a newly created threadS
    df_data_vol_unsync = Get_Vol_snapshot(app=current_app._get_current_object(),
                    symbol=symbol, book=book, day_backwards_count=5, entities=entities)


    # Want to use the open_times to plot a chart. So that we know around what time are the trades opened.
    symbol_opentime_trades_unsync = symbol_opentime_trades(app=current_app._get_current_object(),
                                                           symbol=symbol, book=book,
                                                           start_date=opentime_day_backwards,
                                                           entities=entities)


    # Get the history details such as Trade volume and revenue
    Symbol_history_Daily_unsync = Symbol_history_Daily(symbol=symbol, book=book,
                                                       app=current_app._get_current_object(),
                                                       day_backwards_count=15,entities=entities)

    all_trades=all_trades_unsync.result()
    print("all_open_trades_start() Took: {sec}s".format(sec=(datetime.datetime.now() - all_open_trades_start).total_seconds()))
    df_all_trades = pd.DataFrame(all_trades)



    # If both MT4 and MT5 has no floating trades.
    if len(df_all_trades) == 0 and len(country_mt5_res) == 0:
        return_dict = {"H1": [{"Details": "No Trades for {} Found".format(symbol)}],
                               "H2": [{"Details": "No Trades for {} Found".format(symbol)}] }
        return json.dumps(return_dict)



    #
    # # If there are trades on MT5, we want to be able to add it in as well.
    # if len(country_mt5_res) != 0:
    #     return_dict['Hs7'] = country_mt5_res


    # Getting all the MT4 trades.
    if len(df_all_trades) > 0:
        # Do transformation for all subsequent dfs.
        df_all_trades["LOTS"] = df_all_trades["LOTS"].apply(lambda x: float(x))  # Convert from decimal.decimal
        # Use for calculating net volume. Want to know if net buy or sell

        df_all_trades["NET_LOTS"] = df_all_trades.apply(lambda x: x["LOTS"] if x['CMD'] == 0 else -1 * x["LOTS"], axis=1)

        df_all_trades["TOTAL_PROFIT"] = df_all_trades.apply(lambda x: x["CONVERTED_REVENUE"] + x['REBATE'], axis=1)


        # Want only those open trades.
        df_open_trades = df_all_trades[df_all_trades["CLOSE_TIME"] == pd.Timestamp('1970-01-01 00:00:00')].copy()  # Only open trades.


        col2 = ['LIVE', 'LOGIN', 'SYMBOL', "LOTS", 'NET_LOTS', 'COUNTRY', 'GROUP', 'SWAPS', 'PROFIT', 'CONVERTED_REVENUE', 'REBATE']
        col3 = ['COUNTRY', 'GROUP', 'LOTS', 'NET_LOTS', 'CONVERTED_REVENUE', 'REBATE']


        [top_groups, bottom_groups, top_accounts, bottom_accounts,
         total_sum, largest_login, open_by_country] = open_trades_analysis(df_open_trades,
                                                                           book, col2, col3, symbol=symbol, entity=entity)

        # Want to append so that we know it's from MT4
        for ar in [top_groups, bottom_groups, top_accounts, bottom_accounts, largest_login]:
            if len(ar) > 0 and "Comment" not in ar:  # We don't want those that are empty.
                ar.insert(0, 'Server', 'MT4') # Want to add this column to the front of the df


        # , bottom_groups, top_accounts, bottom_accounts,
        # total_sum, largest_login, open_by_country


        # Closed trades for today!
        df_closed_trades = df_all_trades[
            df_all_trades["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00')].copy()  # Only Closed trades.

        # List unpacking from the return of the function.
        [closed_top_accounts, closed_bottom_accounts, total_sum_closed, top_closed_groups,
         bottom_closed_groups, closed_largest_lot_accounts, closed_by_country] = symbol_closed_trades_analysis(
            df_closed_trades, book, symbol, entity=entity)



    else:
        [top_groups, bottom_groups, top_accounts, bottom_accounts,
         total_sum, largest_login, open_by_country] = [pd.DataFrame() for i in range(7)]

        [closed_top_accounts, closed_bottom_accounts, total_sum_closed, top_closed_groups,
         bottom_closed_groups, closed_largest_lot_accounts, closed_by_country] = [pd.DataFrame() for i in range(7)]


    # To Consolidate the MT4 and MT5 Stuff together.
    # FLOATING only.
    top_groups = appending_df_results(df_mt5_group_winning, top_groups, sort_col="CONVERTED_REVENUE", ascending=False)
    bottom_groups = appending_df_results(df_mt5_group_losing, bottom_groups, sort_col="CONVERTED_REVENUE", ascending=True)

    top_accounts = appending_df_results(df_mt5_login_winning, top_accounts, sort_col="CONVERTED_REVENUE", ascending=False)
    bottom_accounts = appending_df_results(df_mt5_login_losing, bottom_accounts, sort_col="CONVERTED_REVENUE", ascending=True)

    largest_login = appending_df_results(df_mt5_login_largestlots, largest_login, sort_col="LOTS", ascending=False)






        # Want to add in MT5 Details here.
    # mt5_symbol_results_unsync = mt5_Query_SQL_mt5_db_engine_query(SQL_Query="""call aaron.`BGI_MT5_Float_Query`()""", unsync_app=current_app._get_current_object())

    # mt5_symbol_results = mt5_symbol_results_unsync.result()
    # mt5_symbol_results_df = color_profit_for_df(mt5_symbol_results, default=[{"Run Results": "No Open Trades"}], words_to_find=[], return_df=True)
    #
    # # print(mt5_symbol_results_df[mt5_symbol_results_df["SYMBOL"] == "{}".format(symbol)])
    # # print(total_sum)
    #
    # if "SYMBOL" in mt5_symbol_results_df and len(mt5_symbol_results_df[mt5_symbol_results_df["SYMBOL"] == "{}".format(symbol)]) == 1:
    #     mt5_columns= [("NET_LOTS", False),  ("FLOATING_LOTS", False),  ("REVENUE", True), ("TODAY_LOTS", False), ("TODAY_REVENUE", False)]
    #     for (c, t) in mt5_columns:
    #         if c in mt5_symbol_results_df:
    #             total_sum[f"{c} [MT5]"] = profit_red_green(mt5_symbol_results_df[c].values[0]) if t else mt5_symbol_results_df[c].values[0]



    # Get the results from unsync
    df_data_vol= df_data_vol_unsync.result()
    #print(df_data_vol)

    # Want to plot the 30 mins-ish Snapshot Open lots of
    plot_title = "{symbol} Total Lots Snapshot".format(symbol=symbol)
    plot_title = plot_title + " ({book} Book)".format(book=book.upper()) if book.lower() != "none" else plot_title
    #print(df_data_vol)
    vol_fig = plot_symbol_book_total(df_data_vol, plot_title)


    # Want to get data for OPEN TIME on all trades in the symbol
    query_start_time = datetime.datetime.now()
    df_opentiming = pd.DataFrame(symbol_opentime_trades_unsync.result())
    #df_opentiming = pd.DataFrame(res)
    #print(df_opentiming)
    #print("Total Opentiming lots: {}".format(df_opentiming['LOTS'].sum()))
    if len(df_opentiming):
        plot_title = "{symbol} OpenTime".format(symbol=symbol)
        plot_title = plot_title + " ({book} Book)".format(book=book.upper()) if book.lower() != "none" else plot_title
        opentime_fig = plot_symbol_opentime(df_opentiming, plot_title)
        #opentime_fig.show()
    else:
        opentime_fig={}


    # The historical data of the symbol by Country/date
    history_daily_data = pd.DataFrame(Symbol_history_Daily_unsync.result())
    if len(history_daily_data) > 0:

        # If it's by symbol. (ie: if it's by country, we want to show all symbols)
        title_symbol_postfix = "({symbol})".format(symbol=symbol) if len(symbol)>1 else ""

        chart_title_1 = "History Daily Volume" +  title_symbol_postfix
        chart_1_color = "COUNTRY" if  len(symbol) != 0 else "SYMBOL"
        group_by = ["DATE"] + [chart_1_color]   # The columns in which we want to group by.

        history_daily_vol_fig = plot_symbol_history(df=history_daily_data,
                                                    by="Volume",
                                   chart_title=chart_title_1, color_by=chart_1_color,
                                                    group_by=group_by)

        chart_title_2 = "History Daily Revenue" + title_symbol_postfix
        "({symbol})".format(symbol=symbol) if len(symbol) > 1 else ""
        history_daily_rev_fig = plot_symbol_history(df=history_daily_data,
                                                    by="Revenue",
                                                    chart_title=chart_title_2,  color_by=chart_1_color,
                                                    group_by=group_by)

    else:   # If there are no data. We return an empty chart
        history_daily_vol_fig = {}
        history_daily_rev_fig = {}


    return_vals = {"Hs5": open_by_country.to_dict("records") if len(open_by_country)>0 else [{"Comment": "No MT4 Floating Trades"}],
                    "Hs7": [total_sum.to_dict()] if symbol == None else country_mt5_res,
                    "Hs1": top_groups.to_dict("records"),
                    "Hs2": bottom_groups.to_dict("records"),
                    "H1": top_accounts.to_dict("records"),
                    "H2" : bottom_accounts.to_dict("records"),
                    "H5" : largest_login.to_dict("records"),
                    "P1" : vol_fig,
                    "P2": opentime_fig,
                    "H3": closed_top_accounts.to_dict("records"),
                    "H4": closed_bottom_accounts.to_dict("records"),
                    "H6" : closed_largest_lot_accounts.to_dict("records"),
                    "Hs3": top_closed_groups.to_dict("records"),
                    "Hs4" : bottom_closed_groups.to_dict("records"),
                    "P3": history_daily_vol_fig,
                    "P4": history_daily_rev_fig,
                    "V2": [total_sum_closed.to_dict()] if len(total_sum_closed) > 0 else [],
                    "Hs6" : closed_by_country.to_dict("records")
                    }

    #print(return_vals)
    # want to make sure we return something in the values.
    return_vals = {k:d for k,d in return_vals.items() if not (d==None or d==[] or d=={})}

    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps(return_vals, cls=plotly.utils.PlotlyJSONEncoder)



# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@analysis.route('/BGI_Symbol_Float/Country_Open/<country>', methods=['GET', 'POST'])
@roles_required()
def Country_float_trades(country=""):

    title = "{}".format(country.upper())
    header = "{} Trades".format(country.upper())

    # if book.lower() == "b":
    #     header += "(B 📘)"
    # elif book.lower() == "a":
    #     header += "(A 📕)"

    table_ledgend = "COUNTRY: Country that Client Group is in.<br>" + \
                    "GROUP: Client Group.<br>" + \
                    "LOTS : Lots of trades (Or total sum, where applies).<br>" + \
                    "NET LOTS : Cross tally of buy (+ve) and sell (-ve).<br>" + \
                    "CONVERTED REVENUE : SWAPS + PROFIT converted to USD.<br>" + \
                    "REBATE : Amount (Sum) of rebate paid out.<br>" + \
                    "SWAPS : Amount(Sum) of swaps for trades.<br>" + \
                    "PROFIT : PnL (Sum) for trades.<br>" + \
                    "TOTAL PROFIT: CONVERTED REVENUE - REBATE. This is how much BGI Earns.<br>" + \
                    "<br><br>" + \
                    "REBATE will be highlighted if REVENUE is -ve, But REVENUE + REBATE >= 0.<br>" +\
                    "That's to say, Client is still profitable."

    description = Markup("Showing Open trades for {}<br>Details are on Client side.<br><br>{}".format(country, table_ledgend))

    if country == "" :  # There are no information.
        flash("There were no country details.")
        return redirect(url_for("main_app.index"))

    tableau_url = Entity_Float_Viz(Entity=country)

    # Table names will need be in a dict, identifying if the table should be horizontal or vertical.
    # Will try to do smaller vertical table to put 2 or 3 tables in a row.
    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("Country_float_trades"), icon="",
                           Table_name={ "Winning Floating Groups (Client Side)": "Hs1",
                                        "Losing Floating Groups (Client Side)": "Hs2",
                                        "Winning Floating Accounts (Client Side)": "H1",
                                        "Losing Floating Accounts (Client Side)": "H2",
                                        "Largest Lots Floating Accounts (Client Side)": "H5",
                                        "Total Volume Snapshot" : "P1",
                                        "Open Time vs Lots": "P2",
                                        "Total Floating (BGI Side)": "V1",
                                        "Symbol Floating (BGI Side)": "Hs5",
                                        "Line": "Hr1",
                                        "Winning Realised Accounts Today (Client Side)": "H3",
                                        "Losing Realised Accounts Today (Client Side)": "H4",
                                        "Largest Lots Realised Accounts Today (Client Side)": "H6",
                                        "Winning Realised Group Today (Client Side)": "Hs3",
                                        "Losing Realised Group Today (Client Side)": "Hs4",
                                        "History Daily Closed Vol": "P3",
                                        "History Daily Revenue": "P4",
                                        "Total Closed Today (BGI Side)": "V2",
                                        "Symbol Closed (BGI Side)": "Hs6",
                                        "Line2": "Hr2",
                                        },
                           title=title,
                           ajax_url=url_for('analysis.symbol_float_trades_ajax', _external=True, entity=country, book="none", symbol="none"),
                           book = "None",
                           header=header, symbol=country,
                           description=description, no_backgroud_Cover=True,
                           tableau_url=tableau_url,
                           replace_words=Markup(["Today"]))






# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@analysis.route('/BGI_Symbol_Float/Group_Open/<group>', methods=['GET', 'POST'])
@roles_required()
def group_float_trades(group=""):

    title = "{}".format(group.upper())
    header = "{} Trades".format(group.upper())

    # if book.lower() == "b":
    #     header += "(B 📘)"
    # elif book.lower() == "a":
    #     header += "(A 📕)"

    table_ledgend = "COUNTRY: Country that Client Group is in.<br>" + \
                    "GROUP: Client Group.<br>" + \
                    "LOTS : Lots of trades (Or total sum, where applies).<br>" + \
                    "NET LOTS : Cross tally of buy (+ve) and sell (-ve).<br>" + \
                    "CONVERTED REVENUE : SWAPS + PROFIT converted to USD.<br>" + \
                    "REBATE : Amount (Sum) of rebate paid out.<br>" + \
                    "SWAPS : Amount(Sum) of swaps for trades.<br>" + \
                    "PROFIT : PnL (Sum) for trades.<br>" + \
                    "TOTAL PROFIT: CONVERTED REVENUE - REBATE. This is how much BGI Earns.<br>" + \
                    "<br><br>" + \
                    "REBATE will be highlighted if REVENUE is -ve, But REVENUE + REBATE >= 0.<br>" +\
                    "That's to say, Client is still profitable."

    description = Markup("Showing Open trades for {}<br>Details are on Client side.<br><br>{}".format(group.upper(), table_ledgend))

    if group == "" :  # There are no information.
        flash("There were no group details.")
        return redirect(url_for("main_app.index"))

    #tableau_url = Entity_Float_Viz(group=group)

    # Table names will need be in a dict, identifying if the table should be horizontal or vertical.
    # Will try to do smaller vertical table to put 2 or 3 tables in a row.
    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("group_float_trades"), icon="",

                           Table_name={
                                    "Client Group Details": "V2",
                                    "Past Month PnL (Client Side)": "Hs3",
                                    "Winning Floating Accounts (Client Side)": "Hs1",
                                    "Losing Floating Accounts (Client Side)": "Hs2",
                                    "Largest Lots Floating Accounts (Client Side)": "H5",
                                    "Symbol Floating (BGI Side)": "Hs5",
                                    "Total Floating (BGI Side)": "V1",
                                    "Line": "Hr1"
                                        },
                           title=title,
                           ajax_url=url_for('analysis.group_float_trades_ajax', _external=True, group=group),
                           book = "None",
                           header=header, symbol=group,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"]))



# The Ajax call for the symbols we want to query. B Book.
@analysis.route('/Open_Symbol/symbol_float_trades_ajax/<group>', methods=['GET', 'POST'])
@roles_required()
def group_float_trades_ajax(group=""):

    start_time = datetime.datetime.now() # Want to get the datetime when this request was called.
    # Want to show 2 days back if the hour is less than 10am (SGT), else, shows 1 day only.
    opentime_day_backwards_count = 3 if datetime.datetime.now().hour < 12 else 2  # Not too many days, else it will be too small.
    opentime_day_backwards = "{}".format(get_working_day_date(datetime.date.today(), -1 * opentime_day_backwards_count))


    #print("symbol_float_trades_ajax Running.. ")



    all_open_trades_start = datetime.datetime.now()

    # Want to reduce the overheads
    # We can't do this in the threads.
    ServerTimeDiff_Query = "{}".format(session["live1_sgt_time_diff"]) if "live1_sgt_time_diff" in session \
        else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"


    ## Want to find out if the client Group exist.
    sql_query = """SELECT *  FROM live5.`group_table` WHERE `GROUP` = '{}'""".format(group)
    group_details = Query_SQL_db_engine(sql_query)
    if len(group_details) == 0:
        #print("Group Does not exist")
        return json.dumps({"Hs1": [{"Error" : "Client Group Does not Exists. "}]},  cls=plotly.utils.PlotlyJSONEncoder)
    elif  len(group_details) > 1:
        #print("There are more than 1 Client group with that name.")
        return json.dumps({"Hs1": [{"Error": "There are more than 1 Client group with that name."}]}, cls=plotly.utils.PlotlyJSONEncoder)
    # else:
    #     print(group_details)





    all_trades_unsync = group_symbol_all_open_trades(app=current_app._get_current_object(), ServerTimeDiff_Query=ServerTimeDiff_Query,group=group)



    # live here = "live1", not just the number, since the SQL returns such.
    Client_Group_unsync = Client_Group_Details(app=current_app._get_current_object(), live=group_details[0]["LIVE"], group=group)

    Client_Group_Past_PnL_unsync = Client_Group_past_trades(app=current_app._get_current_object(), live=group_details[0]["LIVE"], group=group)

    # # Snap shot of the volume.
    # # Need to pass in the app as this would be using a newly created threadS
    # df_data_vol_unsync = Get_Vol_snapshot(app=current_app._get_current_object(),
    #                 symbol=symbol, book=book, day_backwards_count=5, entities=entities)


    # # Want to use the open_times to plot a chart. So that we know around what time are the trades opened.
    # symbol_opentime_trades_unsync = symbol_opentime_trades(app=current_app._get_current_object(),
    #                                                        symbol=symbol, book=book,
    #                                                        start_date=opentime_day_backwards,
    #                                                        entities=entities)
    #
    #
    # # Get the history details such as Trade volume and revenue
    # Symbol_history_Daily_unsync = Symbol_history_Daily(symbol=symbol, book=book,
    #                                                    app=current_app._get_current_object(),
    #                                                    day_backwards_count=15,entities=entities)

    all_trades=all_trades_unsync.result()
    #print("all_open_trades_start() Took: {sec}s".format(sec=(datetime.datetime.now() - all_open_trades_start).total_seconds()))
    df_all_trades = pd.DataFrame(all_trades)

    # To make it more printable.
    # df_all_trades["OPEN_TIME"] = df_all_trades["OPEN_TIME"].apply(lambda x: "{}".format(x))
    # df_all_trades["CLOSE_TIME"] = df_all_trades["CLOSE_TIME"].apply(lambda x: "{}".format(x))
    print(df_all_trades.columns)
    print(df_all_trades)
    if len(df_all_trades) == 0:
        df_open_trades = pd.DataFrame([])
    else:

        df_all_trades["LOTS"] = df_all_trades["LOTS"].apply(lambda x: float(x))  # Convert from decimal.decimal
        # Use for calculating net volume. Want to know if net buy or sell
        df_all_trades["NET_LOTS"] = df_all_trades.apply(lambda x: x["LOTS"] if x['CMD'] == 0 else -1 * x["LOTS"], axis=1)

        df_all_trades["TOTAL_PROFIT"] = df_all_trades.apply(lambda x: x["CONVERTED_REVENUE"] + x['REBATE'], axis=1)


        # Want only those open trades.
        df_open_trades = df_all_trades[df_all_trades["CLOSE_TIME"] == pd.Timestamp('1970-01-01 00:00:00')].copy()  # Only open trades.


    col2 = ['LIVE', 'LOGIN', "LOTS", 'NET_LOTS', 'SWAPS', 'PROFIT', 'CONVERTED_REVENUE', 'REBATE'] #'COUNTRY', 'GROUP',
    col3 = ['LOTS', 'NET_LOTS', 'CONVERTED_REVENUE', 'REBATE']  #'COUNTRY', 'GROUP',


    # Information from the previous SQL call.
    # We want to determine if the client group is A book or B book.
    book = "b" if group_details[0]["BOOK"].lower() not in ("a", "dealing") else "a"

    client_group_details_data = Client_Group_unsync.result()
    #print(client_group_details_data)

    Client_Group_Past_PnL = Client_Group_Past_PnL_unsync.result()

    [top_accounts, bottom_accounts,
     total_sum, largest_login, open_by_country] = group_open_trades_analysis(df_open_trades, book, col2, col3, group=group)


    return json.dumps({"V2": client_group_details_data,
                       "Hs3" : Client_Group_Past_PnL.to_dict("record"),
                      "Hs1": top_accounts.to_dict("record"),
                      "Hs2" : bottom_accounts.to_dict("record"),
                      "H5" : largest_login.to_dict("record"),
                       "V1": [total_sum.to_dict()],
                      "Hs5" : open_by_country.to_dict("record")
                        },  cls=plotly.utils.PlotlyJSONEncoder)
    #
    # if len(df_all_trades) <= 0:
    #     return json.dumps({"H1": [{"Error": "No Trades for {} Found".format(symbol)}],
    #                        "H2": [{"Error": "No Trades for {} Found".format(symbol)}] })
    #
    # # Do transformation for all subsequent dfs.
    # df_all_trades["LOTS"] = df_all_trades["LOTS"].apply(lambda x: float(x))  # Convert from decimal.decimal
    # # Use for calculating net volume. Want to know if net buy or sell
    # df_all_trades["NET_LOTS"] = df_all_trades.apply(lambda x: x["LOTS"] if x['CMD'] == 0 else -1 * x["LOTS"], axis=1)
    #
    # df_all_trades["TOTAL_PROFIT"] = df_all_trades.apply(lambda x: x["CONVERTED_REVENUE"] + x['REBATE'], axis=1)
    #
    #
    # # Want only those open trades.
    # df_open_trades = df_all_trades[df_all_trades["CLOSE_TIME"] == pd.Timestamp('1970-01-01 00:00:00')].copy()  # Only open trades.
    #
    #
    #
    # col2 = ['LIVE', 'LOGIN', 'SYMBOL', "LOTS", 'NET_LOTS', 'COUNTRY', 'GROUP', 'SWAPS', 'PROFIT', 'CONVERTED_REVENUE', 'REBATE']
    # col3 = ['COUNTRY', 'GROUP', 'LOTS', 'NET_LOTS', 'CONVERTED_REVENUE', 'REBATE']
    #
    #
    # [top_groups, bottom_groups, top_accounts, bottom_accounts,
    #  total_sum, largest_login, open_by_country] = open_trades_analysis(df_open_trades,
    #                                                                    book, col2, col3, symbol=symbol, entity=entity)
    #
    #
    # # Closed trades for today!
    # df_closed_trades = df_all_trades[df_all_trades["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00')].copy()  # Only Closed trades.
    #
    #
    # # List unpacking from the return of the function.
    # [closed_top_accounts, closed_bottom_accounts, total_sum_closed, top_closed_groups,
    #  bottom_closed_groups, closed_largest_lot_accounts, closed_by_country] = symbol_closed_trades_analysis(df_closed_trades, book, symbol, entity=entity)
    #
    #
    # # Get the results from unsync
    # df_data_vol= df_data_vol_unsync.result()
    # #print(df_data_vol)
    #
    # # Want to plot the 30 mins-ish Snapshot Open lots of
    # plot_title = "{symbol} Total Lots Snapshot".format(symbol=symbol)
    # plot_title = plot_title + " ({book} Book)".format(book=book.upper()) if book.lower() != "none" else plot_title
    # vol_fig = plot_symbol_book_total(df_data_vol, plot_title)
    #
    #
    # # Want to get data for OPEN TIME on all trades in the symbol
    # query_start_time = datetime.datetime.now()
    # df_opentiming = pd.DataFrame(symbol_opentime_trades_unsync.result())
    # #df_opentiming = pd.DataFrame(res)
    # #print(df_opentiming)
    # #print("Total Opentiming lots: {}".format(df_opentiming['LOTS'].sum()))
    # if len(df_opentiming):
    #     plot_title = "{symbol} OpenTime".format(symbol=symbol)
    #     plot_title = plot_title + " ({book} Book)".format(book=book.upper()) if book.lower() != "none" else plot_title
    #     opentime_fig = plot_symbol_opentime(df_opentiming, plot_title)
    #     #opentime_fig.show()
    # else:
    #     opentime_fig={}
    #
    #
    # # The historical data of the symbol by Country/date
    # history_daily_data = pd.DataFrame(Symbol_history_Daily_unsync.result())
    # if len(history_daily_data) > 0:
    #
    #     # If it's by symbol. (ie: if it's by country, we want to show all symbols)
    #     title_symbol_postfix = "({symbol})".format(symbol=symbol) if len(symbol)>1 else ""
    #
    #     chart_title_1 = "History Daily Volume" +  title_symbol_postfix
    #     chart_1_color = "COUNTRY" if  len(symbol) != 0 else "SYMBOL"
    #     group_by = ["DATE"] + [chart_1_color]   # The columns in which we want to group by.
    #
    #     history_daily_vol_fig = plot_symbol_history(df=history_daily_data,
    #                                                 by="Volume",
    #                                chart_title=chart_title_1, color_by=chart_1_color,
    #                                                 group_by=group_by)
    #
    #     chart_title_2 = "History Daily Revenue" + title_symbol_postfix
    #     "({symbol})".format(symbol=symbol) if len(symbol) > 1 else ""
    #     history_daily_rev_fig = plot_symbol_history(df=history_daily_data,
    #                                                 by="Revenue",
    #                                                 chart_title=chart_title_2,  color_by=chart_1_color,
    #                                                 group_by=group_by)
    #
    # else:   # If there are no data. We return an empty chart
    #     history_daily_vol_fig = {}
    #     history_daily_rev_fig = {}
    #
    #
    # # Return the values as json.
    # # Each item in the returned dict will become a table, or a plot
    # return json.dumps({"Hs1": top_groups.to_dict("record"),
    #                    "Hs2": bottom_groups.to_dict("record"),
    #                    "H1": top_accounts.to_dict("record"),
    #                    "H2" : bottom_accounts.to_dict("record"),
    #                    "H5" : largest_login.to_dict("record"),
    #                    "P1" : vol_fig,
    #                    "P2": opentime_fig,
    #                    "V1": [total_sum.to_dict()],
    #                    "Hs5" : open_by_country.to_dict("record"),
    #                    "H3": closed_top_accounts.to_dict("record"),
    #                    "H4": closed_bottom_accounts.to_dict("record"),
    #                    "H6" : closed_largest_lot_accounts.to_dict("record"),
    #                    "Hs3": top_closed_groups.to_dict("record"),
    #                    "Hs4" : bottom_closed_groups.to_dict("record"),
    #                    "P3": history_daily_vol_fig,
    #                    "P4": history_daily_rev_fig,
    #                    "V2": [total_sum_closed.to_dict()],
    #                    "Hs6" : closed_by_country.to_dict("record")
    #                    }, cls=plotly.utils.PlotlyJSONEncoder)



# Consolidated function to use for closed trades (All logins for the same symbol)
def symbol_closed_trades_analysis(df, book, symbol, entity="none"):


    # Column that we need.
    col2 = ['LIVE', 'LOGIN', 'SYMBOL', "LOTS", 'NET_LOTS', 'COUNTRY', 'GROUP', 'SWAPS', 'PROFIT', 'CONVERTED_REVENUE', 'REBATE', 'DURATION_(AVG)']
    col3 = ['COUNTRY', 'GROUP', 'LOTS', 'NET_LOTS', 'CONVERTED_REVENUE', 'REBATE']

    # If we are sorting only 1 entity, there is no need to keep showing it.
    if entity != "none":
        if "COUNTRY" in col2:
            col2.remove("COUNTRY")
        if "COUNTRY" in col3:
            col3.remove("COUNTRY")

    # There are no closed trades for the day yet
    if len(df) <=0:
        empty_return  = pd.DataFrame([{"Note": "There are no closed trades for the day for {} yet".format(symbol)}])
        return [empty_return] * 7
    else:
        # Use for calculating the total seconds that the trades are opened for.
        df["DURATION_(AVG)"] = df.apply(lambda x: (x["CLOSE_TIME"] - x["OPEN_TIME"]).total_seconds(), axis=1)

        # Want to take the mean duration, by trade.
        closed_login_sum = df.groupby(by=['LIVE', 'LOGIN', 'COUNTRY', 'GROUP', 'SYMBOL']).agg({'LOTS': 'sum',
                                                                            'NET_LOTS': 'sum',
                                                                            'CONVERTED_REVENUE': 'sum',
                                                                            'PROFIT': 'sum',
                                                                            'SWAPS': 'sum',
                                                                            'TOTAL_PROFIT' : 'sum',
                                                                            'REBATE' : 'sum',
                                                                            'DURATION_(AVG)' : 'mean'}).reset_index()

        # Round off the values that is not needed.
        closed_login_sum["LOTS"] = round(closed_login_sum['LOTS'],2)
        closed_login_sum["NET_LOTS"] = round(closed_login_sum['NET_LOTS'], 2)
        closed_login_sum["CONVERTED_REVENUE"] = round(closed_login_sum['CONVERTED_REVENUE'], 2)
        closed_login_sum["PROFIT"] = round(closed_login_sum['PROFIT'], 2)
        #closed_login_sum["PROFIT"] = round(closed_login_sum['PROFIT'], 2)
        closed_login_sum["REBATE"] = closed_login_sum.apply( lambda x: color_rebate(rebate=x['REBATE'], \
                                                                                    pnl=x["CONVERTED_REVENUE"]), axis=1)
        #closed_login_sum["SWAPS"] = round(closed_login_sum['SWAPS'], 2)
        # Want to color the SWAPS and PROFIT.
        closed_login_sum["SWAPS"] = closed_login_sum["SWAPS"].apply(profit_red_green)
        closed_login_sum["PROFIT"] = closed_login_sum["PROFIT"].apply(profit_red_green)

        closed_login_sum["LOGIN"] = closed_login_sum.apply(lambda x: live_login_analysis_url( \
            Live=x['LIVE'].lower().replace("live", ""), Login=x["LOGIN"]), axis=1)
        # Want to get the average of the duration.
        closed_login_sum["DURATION_(AVG)"] = closed_login_sum["DURATION_(AVG)"].apply(lambda x: trade_duration_bin(x))

        closed_login_sum["GROUP"] = closed_login_sum["GROUP"].apply(client_group_url)    # Get the client Group URL link

        # Want the Closed Top/Bottom accounts. Top = Winning, so no -ve PnL.
        closed_top_accounts = closed_login_sum[closed_login_sum['CONVERTED_REVENUE'] >= 0].sort_values(\
                                                                'CONVERTED_REVENUE', ascending=False)[col2].head(20)
        closed_bottom_accounts = closed_login_sum[closed_login_sum['CONVERTED_REVENUE'] < 0].sort_values(\
                                                                'CONVERTED_REVENUE', ascending=True)[col2].head(20)

        closed_largest_lot_accounts = closed_login_sum.sort_values('LOTS', ascending=False)[col2].head(20)


        # Color the CONVERTED_REVENUE
        closed_bottom_accounts["CONVERTED_REVENUE"] = closed_bottom_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))
        closed_top_accounts["CONVERTED_REVENUE"] = closed_top_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))
        closed_largest_lot_accounts["CONVERTED_REVENUE"] = closed_largest_lot_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        # If there are either no winning Accounts, or no losing accounts.
        # No winning accounts with closed trades for today
        closed_top_accounts = pd.DataFrame(
            [{"Comment": "There are currently no Accounts With Closed Winning PnL for today for {}".format(symbol)}]) if \
            len(closed_top_accounts) <= 0 else closed_top_accounts
        # No losing accounts for closed trades for today.
        closed_bottom_accounts = pd.DataFrame(
            [{"Comment": "There are currently no Accounts With Closed Losing PnL for today for {}".format(symbol)}]) if \
            len(closed_bottom_accounts) <= 0 else closed_bottom_accounts

        closed_largest_lot_accounts = pd.DataFrame(
            [{"Comment": "There are currently no Accounts With Closed Losing PnL for today for {}".format(symbol)}]) if \
            len(closed_largest_lot_accounts) <= 0 else closed_largest_lot_accounts

        # Closed Trades for today, Group by client Group and Country
        closed_group_sum = df.groupby(by=['COUNTRY', 'GROUP']).sum().reset_index()
        closed_group_sum["LOTS"] = round(closed_group_sum['LOTS'],2)
        closed_group_sum["NET_LOTS"] = round(closed_group_sum['NET_LOTS'], 2)
        closed_group_sum["CONVERTED_REVENUE"] = round(closed_group_sum['CONVERTED_REVENUE'], 2)
        closed_group_sum["REBATE"] = closed_group_sum.apply( lambda x: color_rebate(rebate=x['REBATE'], \
                                                                                    pnl=x["CONVERTED_REVENUE"]), axis=1)

        # Get the Client Group URL link.
        closed_group_sum["GROUP"] = closed_group_sum["GROUP"].apply(client_group_url)

        # Only want those that are profitable
        top_closed_groups = closed_group_sum[closed_group_sum['CONVERTED_REVENUE']>=0].sort_values('CONVERTED_REVENUE', \
                                                                              ascending=False)[col3].head(20)
        top_closed_groups["CONVERTED_REVENUE"] = top_closed_groups["CONVERTED_REVENUE"].apply(
            lambda x: profit_red_green(x))
        top_closed_groups = pd.DataFrame([{"Comment": "There are currently no groups with closed profit for {}".format(symbol)}]) if \
            len(top_closed_groups) <= 0 else top_closed_groups

        # Only want those that are making a loss
        bottom_closed_groups = closed_group_sum[closed_group_sum['CONVERTED_REVENUE']<=0].sort_values('CONVERTED_REVENUE', \
                                                                                                      ascending=True)[col3].head(20)
        bottom_closed_groups["CONVERTED_REVENUE"] = bottom_closed_groups["CONVERTED_REVENUE"].apply(
            lambda x: profit_red_green(x))
        bottom_closed_groups = pd.DataFrame(
            [{"Comment": "There are currently no groups with floating losses for {}".format(symbol)}]) if \
            len(bottom_closed_groups) <= 0 else bottom_closed_groups

        # Total sum Floating
        total_sum_closed_col = ['LOTS','PROFIT', 'SWAPS', 'CONVERTED_REVENUE',  'REBATE' ]
        total_sum_closed = df[total_sum_closed_col].sum()


        if book == "b":  # Only want to flip sides when it's B book.
            total_sum_closed["REBATE"] = color_rebate(rebate=total_sum_closed['REBATE'],
                                                      pnl=total_sum_closed["CONVERTED_REVENUE"], multiplier=-1)
            total_sum_closed = total_sum_closed.apply(lambda x: round(x * -1, 2) if isfloat(x) else x)  # Flip it to be on BGI Side.
        else:  # If it's A book. We don't need to do that.
            total_sum_closed = total_sum_closed.apply(lambda x: round(x , 2))  # Flip it to be on BGI Side.

        total_sum_closed["LOTS"] = abs(total_sum_closed["LOTS"])  # Since it's Total lots, we only want the abs value

        # Want to print it properly with colors
        total_sum_closed["CONVERTED_REVENUE"] = profit_red_green(total_sum_closed["CONVERTED_REVENUE"])
        total_sum_closed["PROFIT"] = profit_red_green(total_sum_closed["PROFIT"])
        total_sum_closed["SWAPS"] = profit_red_green(total_sum_closed["SWAPS"])


        for c in total_sum_closed_col: # Want to print it properly.
            if isfloat(total_sum_closed[c]):
                total_sum_closed[c] = "{:,}".format(total_sum_closed[c])

        if entity == "none":
            # Want the table by country.
            closed_by_country = df.groupby(["COUNTRY"])[[ 'LOTS', 'NET_LOTS','PROFIT','CONVERTED_REVENUE',
                                                                    'REBATE', 'TOTAL_PROFIT']].sum().reset_index()
        else:
            df["SYMBOL"] = df["SYMBOL"].apply(split_root_symbol)
            # Want the table by country.
            closed_by_country = df.groupby(["SYMBOL"])[['LOTS', 'NET_LOTS', 'PROFIT', 'CONVERTED_REVENUE',
                                                         'REBATE', 'TOTAL_PROFIT']].sum().reset_index()


        # Want to show Net Lots on BGI Side
        closed_by_country["NET_LOTS"] =  closed_by_country["NET_LOTS"] * ( -1 if book.lower() != "a" else 1)

        closed_by_country["REBATE"] = closed_by_country.apply(lambda x: color_rebate(rebate=x['REBATE'], pnl=x["CONVERTED_REVENUE"], multiplier=-1),
                                              axis=1)

        # Want to show BGI Side. Color according to BGI Side
        closed_by_country["TOTAL_PROFIT"] = closed_by_country["TOTAL_PROFIT"].apply(lambda x: profit_red_green(x * ( -1 if book.lower() != "a" else 1)))

        # Only want to flip it if it's -1.
        closed_by_country["CONVERTED_REVENUE"] = closed_by_country["CONVERTED_REVENUE"].apply( lambda x: profit_red_green(x * ( -1 if book.lower() != "a" else 1)))


        closed_by_country["LOTS"] = abs(closed_by_country["LOTS"])

        closed_by_country["PROFIT"] = closed_by_country["PROFIT"] * ( -1 if book.lower() != "a" else 1)
        closed_by_country.sort_values(["NET_LOTS"], inplace=True)    # Sort it by Net_Lots
        closed_by_country = closed_by_country.head(20)  # Want to only show the top 20, by net lots.

        closed_by_country = pd.DataFrame(
            [{"Comment": "There are currently no Country with floating PnL for {}".format(symbol)}]) if \
            len(closed_by_country) <= 0 else closed_by_country

    return [closed_top_accounts,  closed_bottom_accounts , total_sum_closed, top_closed_groups,
            bottom_closed_groups, closed_largest_lot_accounts, closed_by_country]

# To return the open trades analysis.
# col is for larger tables, showing logins and such
# col_1 is for grouped 'group' tables,
def open_trades_analysis(df_open_trades, book, col, col_1, symbol="", entity="none", group=""):

    # Want the display line should be.
    display_line = ""
    #symbol if len(symbol) > 0 else entity
    if len(symbol) > 0:
        display_line = symbol
    elif entity != "none":
        display_line = entity
    else:
        display_line = group


    if len(df_open_trades) <= 0:    # If there are no closed trades for the day.
        return_df = pd.DataFrame([{"Note": "There are no open trades for {} now".format(display_line)}])


        return [return_df] * 7   # return 7 empty data frames.
        #[top_groups, bottom_groups, top_accounts , bottom_accounts, total_sum, largest_login , open_by_country]
    else:
        #print("entity: '{}'".format(entity))

        # If we are already showing by entity, we don't need to keep repeating.
        # Will remove the COUNTRY column
        if entity != "none":
            if "COUNTRY" in col:
                col.remove("COUNTRY")
                #print("Removing Country from col")
            if "COUNTRY" in col_1:
                col_1.remove("COUNTRY")
                #print("Removing Country from col_1")

        # Group the trades together.
        live_login_sum = df_open_trades.groupby(by=['LIVE', 'LOGIN', 'COUNTRY', 'GROUP', 'SYMBOL']).sum().reset_index()

        #print(live_login_sum)
        # Round off the values that is not needed.
        live_login_sum["LOTS"] = round(live_login_sum['LOTS'],2)
        live_login_sum["NET_LOTS"] = round(live_login_sum['NET_LOTS'], 2)
        live_login_sum['REBATE'] = live_login_sum.apply(lambda x: color_rebate(rebate=x['REBATE'], pnl=x["CONVERTED_REVENUE"]), axis=1)
        live_login_sum["CONVERTED_REVENUE"] = round(live_login_sum['CONVERTED_REVENUE'], 2)
        #live_login_sum["PROFIT"] = round(live_login_sum['PROFIT'], 2)
        live_login_sum["PROFIT"] = live_login_sum["PROFIT"].apply(profit_red_green)
        live_login_sum["SWAPS"] = live_login_sum["SWAPS"].apply(profit_red_green)
            #round(live_login_sum['SWAPS'], 2)
        live_login_sum["LOGIN"] = live_login_sum.apply(lambda x: live_login_analysis_url(\
                                    Live=x['LIVE'].lower().replace("live", ""), Login=x["LOGIN"]), axis=1)
        live_login_sum["TOTAL_PROFIT"] = round(live_login_sum['TOTAL_PROFIT'], 2)

        live_login_sum["GROUP"] =  live_login_sum["GROUP"].apply(client_group_url)



        # Want Top and winning accounts. If there are none. we will reflect accordingly.
        top_accounts = live_login_sum[live_login_sum['CONVERTED_REVENUE'] >= 0 ].sort_values('CONVERTED_REVENUE', ascending=False)[col].head(20)
        # Color the CONVERTED_REVENUE
        top_accounts["CONVERTED_REVENUE"] = top_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        top_accounts = pd.DataFrame([{"Comment": "There are currently no client with floating profit for {}".format(symbol)}]) \
                        if len(top_accounts) <= 0 else top_accounts



        # Want bottom and Loosing accounts. If there are none, we will reflect it accordingly.
        bottom_accounts = live_login_sum[live_login_sum['CONVERTED_REVENUE'] < 0 ].sort_values('CONVERTED_REVENUE', ascending=True)[col].head(20)
        # Color the CONVERTED_REVENUE
        bottom_accounts["CONVERTED_REVENUE"] = bottom_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        bottom_accounts = pd.DataFrame(
            [{"Comment": "There are currently no client with floating losses for {}".format(symbol)}]) \
            if len(bottom_accounts) <= 0 else bottom_accounts


        # By entities/Group
        group_sum = df_open_trades.groupby(by=['COUNTRY', 'GROUP',])[['LOTS', 'NET_LOTS',
                                                                      'CONVERTED_REVENUE', 'SYMBOL', 'REBATE',
                                                                      'TOTAL_PROFIT']].sum().reset_index()

        # Want to color the rebate if profit <= 0, but Profit + rebate > 0
        group_sum['REBATE'] = group_sum.apply(lambda x: color_rebate(rebate=x['REBATE'],
                                                        pnl=x["CONVERTED_REVENUE"]), axis=1)
        # Round it off to be able to be printed better.
        group_sum['CONVERTED_REVENUE'] = round(group_sum['CONVERTED_REVENUE'], 2)
        group_sum['LOTS'] = round(group_sum['LOTS'], 2)
        group_sum['NET_LOTS'] = round(group_sum['NET_LOTS'], 2)

        # Get the Group URL link
        group_sum["GROUP"] = group_sum["GROUP"].apply(client_group_url)


        # Only want those that are profitable
        top_groups = group_sum[group_sum['CONVERTED_REVENUE']>=0].sort_values('CONVERTED_REVENUE',
                                                                              ascending=False)[col_1].head(20)
        # Color the CONVERTED_REVENUE
        top_groups["CONVERTED_REVENUE"] = top_groups["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        top_groups = pd.DataFrame([{"Comment": "There are currently no groups with floating profit for {}".format(symbol)}]) if \
            len(top_groups) <= 0 else top_groups


        # Only want those that are making a loss
        bottom_groups = group_sum[group_sum['CONVERTED_REVENUE']<=0].sort_values('CONVERTED_REVENUE', ascending=True)[col_1].head(20)
        # Color the CONVERTED_REVENUE
        bottom_groups["CONVERTED_REVENUE"] = bottom_groups["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        bottom_groups = pd.DataFrame(
            [{"Comment": "There are currently no groups with floating losses for {}".format(symbol)}]) if \
            len(bottom_groups) <= 0 else bottom_groups

        # Total sum Floating
        if entity == "none":     # By symbol. We can show net lots.
            total_sum_Col = ['LOTS', 'NET_LOTS', 'CONVERTED_REVENUE', 'PROFIT', 'SWAPS' , 'REBATE']       # The columns that we want to show
        else:   # Net volume dosn't make sense when we want to show only by entity/country
            total_sum_Col = ['LOTS', 'CONVERTED_REVENUE', 'PROFIT', 'SWAPS', 'REBATE']  # The columns that we want to show


        total_sum = df_open_trades[total_sum_Col].sum()
        total_sum =  total_sum.apply(lambda x: round(x * -1 if book.lower() != 'a' else x, 2)) # Flip it to be on BGI Side if it's not a book.
        total_sum["LOTS"] = "{:,}".format(abs(total_sum["LOTS"]))  # Since it's Total lots, we only want the abs value
        total_sum['REBATE'] = color_rebate(rebate=total_sum['REBATE'], pnl=total_sum["CONVERTED_REVENUE"])
        total_sum["PROFIT"] = profit_red_green(total_sum["PROFIT"])
        total_sum["SWAPS"] = profit_red_green(total_sum["SWAPS"])
        total_sum["CONVERTED_REVENUE"] = profit_red_green(total_sum["CONVERTED_REVENUE"])

        #
        # for c in total_sum_Col: # Want to print it properly.
        #     if isfloat(total_sum[c]):
        #         total_sum[c] = "{:,}".format(total_sum[c])

        if entity != "none":  # If it's by country.
            # Want the table by SYMBOL.

            # Make a copy. Don't want to implement changes on the actal.
            df_open_trades_copy = df_open_trades.copy()
            df_open_trades_copy['SYMBOL'] = df_open_trades_copy['SYMBOL'].apply(split_root_symbol)

            open_by_country = df_open_trades_copy.groupby(["SYMBOL"])[[ 'LOTS', 'NET_LOTS','PROFIT','CONVERTED_REVENUE',
                                                                    'REBATE', 'TOTAL_PROFIT']].sum().reset_index()
            # Releases the memory.
            df_open_trades_copy = pd.DataFrame()
        else:

            # Want the table by country.
            open_by_country = df_open_trades.groupby(["COUNTRY"])[[ 'LOTS', 'NET_LOTS','PROFIT','CONVERTED_REVENUE',
                                                                    'REBATE', 'TOTAL_PROFIT']].sum().reset_index()

        open_by_country["NET_LOTS"] = -1 * open_by_country["NET_LOTS"]

        open_by_country["REBATE"] = open_by_country.apply(lambda x: color_rebate(rebate=x['REBATE'],
                                                            pnl=x["CONVERTED_REVENUE"], multiplier=-1), axis=1)

        # Want to show BGI Side. Color according to BGI Side
        ## Flip it to be on BGI Side if it's not A book.
        open_by_country["TOTAL_PROFIT"] = open_by_country["TOTAL_PROFIT"].apply(lambda x: profit_red_green(x * -1) if \
                                                        book.lower() != "a" else profit_red_green(x))

        if book.lower() != "a": # Only want to flip sides when it's B book.
            open_by_country["CONVERTED_REVENUE"] = open_by_country["CONVERTED_REVENUE"].apply(
                lambda x: profit_red_green(-1 * x))
        else:   # If it's A book. We don't need to do that.
            open_by_country["CONVERTED_REVENUE"] = open_by_country["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))


        open_by_country["LOTS"] = abs(open_by_country["LOTS"])
        # Flip it to be on BGI Side if it's not A book.
        open_by_country["PROFIT"] =  open_by_country["PROFIT"] * (-1 if book.lower() != 'a' else 1)
        open_by_country.sort_values(["NET_LOTS"], inplace=True)    # Sort it by Net_Lots
        open_by_country = open_by_country.head(20)                  # Want to take only the top 20

        open_by_country = pd.DataFrame(
            [{"Comment": "There are currently no Country with floating PnL for {}".format(symbol)}]) if \
            len(open_by_country) <= 0 else open_by_country


        # Largest (lots) Floating Account.
        largest_login = live_login_sum.sort_values('LOTS', ascending=False)[col].head(20)
        # Color the CONVERTED_REVENUE
        largest_login["CONVERTED_REVENUE"] = largest_login["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        largest_login = pd.DataFrame(
            [{"Comment": "There are currently no login with open trades for {}".format(symbol)}]) if \
            len(largest_login) <= 0 else largest_login

    return  [top_groups, bottom_groups, top_accounts , bottom_accounts, total_sum, largest_login , open_by_country]

# # Get all open trades of a particular symbol.
# # Get it converted as well.
# # Can choose A Book or B Book.
@analysis.route('/Past_Trades/<book>/<days>/<symbol>', methods=['GET', 'POST'])
@roles_required()
def symbol_closed_trades(symbol="", book="b", days=-1):

    PnL_day = get_working_day_date(datetime.datetime.now(), int(days))

    title = "{} {}".format(symbol,  PnL_day.date())
    header = "{} {} Trades".format(symbol, PnL_day.date())

    if book.lower() == "b":
        header += "(B 📘)"
    elif book.lower() == "a":
        header += "(A 📕)"


    description = Markup("Showing Closed trades for {} on {}<br>Details are on Client side.<br>Not showing HK PnL.".format(symbol, PnL_day.date()))

    if symbol == "" :  # There are no information.
        flash("There were no symbol details.")
        return redirect(url_for("main_app.index"))

    tableau_url = symbol_yesterday_Viz(symbol=symbol, book=book)

    # Table names will need be in a dict, identifying if the table should be horizontal or vertical.
    # Will try to do smaller vertical table to put 2 or 3 tables in a row.
    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("symbol_closed_trades"),
                           icon="",
                           Table_name={ "Winning Accounts (Client Side)": "H3",
                                        "Losing Accounts (Client Side)": "H4",
                                        "Largest Lots Accounts (Client Side)": "H6",
                                        "Winning Group (Client Side)": "Hs3",
                                        "Losing Group (Client Side)": "Hs4",
                                        "Total Closed (BGI Side)": "V2",
                                        "COUNTRY CLOSED (BGI SIDE)":"Hs6",
                                        "Line2": "Hr2"
                                        },
                           title=title,
                           ajax_url=url_for('analysis.symbol_close_trades_ajax', _external=True, symbol=symbol, book=book, days=days),
                           book = book.upper(),
                           header=header, symbol=symbol,
                           tableau_url=tableau_url,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"]))



# The Ajax call for the symbols we want to query. B Book.
@analysis.route('/Past_Trades/<book>/<days>/<symbol>/symbol_closed_trades_ajax/', methods=['GET', 'POST'])
@roles_required()
def symbol_close_trades_ajax(book="b", symbol="" ,days=-1):



    #col2 = ['LIVE', 'LOGIN', 'SYMBOL', "LOTS", 'NET_LOTS', 'COUNTRY', 'GROUP', 'SWAPS', 'PROFIT', 'CONVERTED_REVENUE']
    #col3 = ['COUNTRY', 'GROUP', 'LOTS', 'NET_LOTS', 'CONVERTED_REVENUE']


    # Closed trades for today, Make it into a pandas list.
    df_closed_trades = pd.DataFrame(symbol_get_past_closed_trades(symbol=symbol, book=book, days=days))

    if len(df_closed_trades) > 0:
        df_closed_trades["TOTAL_PROFIT"] = df_closed_trades.apply(lambda x: x["CONVERTED_REVENUE"] + x['REBATE'], axis=1)

        df_closed_trades["LOTS"] = df_closed_trades["LOTS"].apply(float)

        df_closed_trades["NET_LOTS"] = df_closed_trades.apply(lambda x: x["LOTS"] if x['CMD'] == 0 else -1 * x["LOTS"],
                                                              axis=1)
    # List unpacking from the return of the function.
    [closed_top_accounts, closed_bottom_accounts, total_sum_closed, top_closed_groups,
     bottom_closed_groups, closed_largest_lot_accounts, closed_by_country] = symbol_closed_trades_analysis(df_closed_trades, book, symbol)


    # Return the values as json.
    # Each item in the returned dict will become a table, or a plot
    return json.dumps({"H3": closed_top_accounts.to_dict("record"),
                       "H4": closed_bottom_accounts.to_dict("record"),
                       "H6" : closed_largest_lot_accounts.to_dict("record"),
                       "Hs3": top_closed_groups.to_dict("record"),
                       "Hs4" : bottom_closed_groups.to_dict("record"),
                       "Hs6" : closed_by_country.to_dict("record"),
                       "V2": [total_sum_closed.to_dict()]
                       }, cls=plotly.utils.PlotlyJSONEncoder)


def symbol_get_past_closed_trades(symbol="", book="B", days=-1):
    #symbol="XAUUSD"

    backward_days = int(days)   # The backward count days.
    symbol_condition = " AND mt4_trades.SYMBOL Like '%{}%' ".format(symbol)
    country_condition = " AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK') "
    book_condition = " AND group_table.BOOK = '{}'".format(book)

    # Want to reduce the overheads
    # Should be 6 (GMT + 2, 2300-2300) or 7 (GMT + 1, 2200-2200)
    ServerTimeDiff_Query = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()
    #ServerTimeDiff_Query=6
    # If it's Before SG 5 am or 6am, need to count back 1 more day.
    if datetime.datetime.now().hour < (ServerTimeDiff_Query -1):
        backward_days = backward_days -1    # Need to off set by 1 day.


    # Want to calculate the start and end date of the trading day
    start_of_day = get_working_day_date(datetime.datetime.now(), backward_days-1)
    end_of_day = get_working_day_date(datetime.datetime.now(), backward_days)
    end_of_day_live5 = get_working_day_date(datetime.datetime.now(), backward_days+1)

    Live1_startofday = " {} {Start_hour}:00:00".format(start_of_day.strftime("%Y-%m-%d"), Start_hour = 23)
    Live1_endofday = " {} {Start_hour}:00:00".format(end_of_day.strftime("%Y-%m-%d"), Start_hour=23)

    Live5_startofday = " {} 00:00:00".format(end_of_day.strftime("%Y-%m-%d"))
    Live5_endofday = " {} 00:00:00".format(end_of_day_live5.strftime("%Y-%m-%d"))


    if book.lower() == "a":
        # Additional SQL query if a book
        Live2_book_query = """ AND	(
		(mt4_trades.`GROUP` IN(SELECT `GROUP` FROM live2.a_group))
		OR (LOGIN IN(SELECT LOGIN FROM live2.a_login))
		OR LOGIN = '9583'
		OR LOGIN = '9615'
		OR LOGIN = '9618'
		OR(mt4_trades.`GROUP` LIKE 'A_ATG%' AND VOLUME > 1501)
	) """
    else:
        Live2_book_query = book_condition

    sql_statement = """(SELECT 'live1' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`, VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
           ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
        FROM live1.mt4_trades, live5.group_table, live1.symbol_rebate
        WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND  symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live1_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live1_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live1' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition} {country_condition} {book_condition}
            )
    UNION 
        (SELECT 'live2' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live2.mt4_trades,live5.group_table,  live2.symbol_rebate
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live1_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live1_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live2' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition} {country_condition} {Live2_book_query})
    UNION 
        (SELECT 'live3' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live3.mt4_trades,live5.group_table,  live3.symbol_rebate
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live1_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live1_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live3' 
            AND LENGTH(mt4_trades.LOGIN)>4 
            AND mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM live3.cambodia_exclude) 
            {symbol_condition} {country_condition} {book_condition})
    UNION
        (SELECT 'live5' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live5.mt4_trades,live5.group_table,  live5.symbol_rebate 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live5_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live5_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live5' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition} {country_condition} {book_condition})""".format(symbol_condition=symbol_condition, ServerTimeDiff_Query=ServerTimeDiff_Query, \
                                                                               book_condition=book_condition, country_condition=country_condition,\
                                                                               Live2_book_query=Live2_book_query,
                                                                               Live1_startofday=Live1_startofday, Live1_endofday=Live1_endofday,
                                                                               Live5_startofday=Live5_startofday,
                                                                               Live5_endofday=Live5_endofday)

    #print(sql_statement)
    sql_query = text(sql_statement.replace("\n", " ").replace("\t", " "))
    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()          # Column names

    return [dict(zip(result_col, r)) for r in result_data]






def get_group_closed_trades(group, days=-1):
    #symbol="XAUUSD"

    backward_days = int(days)   # The backward count days.

    # Want to reduce the overheads
    # Should be 6 (GMT + 2, 2300-2300) or 7 (GMT + 1, 2200-2200)
    ServerTimeDiff_Query = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()
    #ServerTimeDiff_Query=6
    # If it's Before SG 5 am or 6am, need to count back 1 more day.
    if datetime.datetime.now().hour < (ServerTimeDiff_Query -1):
        backward_days = backward_days -1    # Need to off set by 1 day.


    # Want to calculate the start and end date of the trading day
    start_of_day = get_working_day_date(datetime.datetime.now(), backward_days-1)
    end_of_day = get_working_day_date(datetime.datetime.now(), backward_days)
    end_of_day_live5 = get_working_day_date(datetime.datetime.now(), backward_days+1)

    Live1_startofday = " {} {Start_hour}:00:00".format(start_of_day.strftime("%Y-%m-%d"), Start_hour = 23)
    Live1_endofday = " {} {Start_hour}:00:00".format(end_of_day.strftime("%Y-%m-%d"), Start_hour=23)

    Live5_startofday = " {} 00:00:00".format(end_of_day.strftime("%Y-%m-%d"))
    Live5_endofday = " {} 00:00:00".format(end_of_day_live5.strftime("%Y-%m-%d"))

    group_condition = " and mt4_trades.`GROUP` like '%{}%' ".format(group)

    sql_statement = """(SELECT 'live1' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`, VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
           ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
        FROM live1.mt4_trades, live5.group_table, live1.symbol_rebate
        WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND  symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live1_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live1_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live1' 
            AND LENGTH(mt4_trades.LOGIN)>4 
            {group_condition} 
            )
    UNION 
        (SELECT 'live2' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live2.mt4_trades,live5.group_table,  live2.symbol_rebate
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live1_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live1_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live2' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {group_condition})
    UNION 
        (SELECT 'live3' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live3.mt4_trades,live5.group_table,  live3.symbol_rebate
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live1_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live1_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live3' 
            AND LENGTH(mt4_trades.LOGIN)>4 
            AND mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM live3.cambodia_exclude) 
            {group_condition})
    UNION
        (SELECT 'live5' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        mt4_trades.SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`,VOLUME*0.01 * symbol_rebate.REBATE as REBATE,
            ROUND(CASE 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                WHEN mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) 
            ELSE 0 END,2) AS CONVERTED_REVENUE
            FROM live5.mt4_trades,live5.group_table,  live5.symbol_rebate 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND symbol_rebate.SYMBOL = mt4_trades.SYMBOL AND
            mt4_trades.CLOSE_TIME >= '{Live5_startofday}' AND  mt4_trades.CLOSE_TIME < '{Live5_endofday}'
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live5' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {group_condition})""".format(group_condition=group_condition, ServerTimeDiff_Query=ServerTimeDiff_Query,
    Live1_startofday = Live1_startofday, Live1_endofday = Live1_endofday, Live5_startofday = Live5_startofday,
    Live5_endofday = Live5_endofday)

    #print(sql_statement)
    sql_query = text(sql_statement.replace("\n", " ").replace("\t", " "))
    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()          # Column names

    return [dict(zip(result_col, r)) for r in result_data]












# Want to get the open trades by timing.
def symbol_open_trade_by_timing(symbol="", book=""):

    # symbol="XAUUSD"
    symbol_condition = " AND SYMBOL Like '%{}%' ".format(symbol)
    country_condition = " AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK') "
    book_condition = " AND group_table.BOOK = '{}'".format(book)

    if book.lower() == "a":
        # Additional SQL query if a book
        Live2_book_query = """ AND	(
    		(mt4_trades.`GROUP` IN(SELECT `GROUP` FROM live2.a_group))
    		OR (LOGIN IN(SELECT LOGIN FROM live2.a_login))
    		OR LOGIN = '9583'
    		OR LOGIN = '9615'
    		OR LOGIN = '9618'
    		OR(mt4_trades.`GROUP` LIKE 'A_ATG%' AND VOLUME > 1501)
    	) """
    else:
        Live2_book_query = book_condition


    OPEN_TIME_LIMIT = "{} 23:00:00".format(datetime.date.today() - datetime.timedelta(days=3))



    sql_statement = """ SELECT 	LIVE,
        COUNTRY,
        CMD,
        SUM(LOTS) as 'LOTS',
        OPEN_PRICE,
        OPEN_TIME,
        CLOSE_PRICE,
        CLOSE_TIME,
        SUM(SWAPS) as 'SWAPS',
        SUM(PROFIT) as 'PROFIT',
        `GROUP`
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
            AND LENGTH(mt4_trades.LOGIN)> 4 {symbol_condition} {country_condition} {book_condition}
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
            AND LENGTH(mt4_trades.LOGIN)> 4 {symbol_condition} {country_condition} {Live2_book_query}
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
            AND mt4_trades.LOGIN NOT IN(SELECT LOGIN FROM live3.cambodia_exclude){symbol_condition} {country_condition} {book_condition}
        )
            UNION
        (
            SELECT
                'live5' AS LIVE, group_table.COUNTRY, CMD, VOLUME * 0.01 AS LOTS, OPEN_PRICE,
                OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM
                live5.mt4_trades,
                live5.group_table
            WHERE
                mt4_trades.`GROUP` = group_table.`GROUP`
            AND mt4_trades.OPEN_TIME >= '{OPEN_TIME_LIMIT}'
            AND LENGTH(mt4_trades.SYMBOL)> 0
            AND mt4_trades.CMD < 2
            AND group_table.LIVE = 'live5'
            AND LENGTH(mt4_trades.LOGIN)> 4 {symbol_condition} {country_condition} {book_condition}
        ))AS A
        GROUP BY COUNTRY, LEFT(OPEN_TIME, 16), `GROUP`""".format(OPEN_TIME_LIMIT=OPEN_TIME_LIMIT, symbol_condition=symbol_condition,
                    country_condition=country_condition, book_condition=book_condition,
                    Live2_book_query=Live2_book_query)

    sql_statement.replace("\n", "").replace("\t", " ")
    sql_query = text(sql_statement.replace("\n", " ").replace("\t", " "))
    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()          # Column names
    return [dict(zip(result_col, r)) for r in result_data]
#
# @analysis.route('/analysis/cn_live_vol_ajax')
# @roles_required()
# # Gets the cn df, and uses it to plot the various charts.
# def cn_live_vol_ajax():
#
#     # start = datetime.datetime.now()
#     # df = get_cn_df()
#     # bar = plot_open_position_net(df, chart_title = "[CN] Open Position")
#     # bar = plot_open_position_net(df, chart_title = "[CN] Open Position")
#     # cn_pnl_bar = plot_open_position_revenue(df, chart_title="[CN] Open Position Revenue")
#     # cn_heat_map = plot_volVSgroup_heat_map(df,chart_title="[CN] Net Position by Group")
#     # print("Getting cn df and charts {} Seconds.".format((datetime.datetime.now()-start).total_seconds()))
#     # vol_sum = round(sum(df['VOLUME']),2)
#     # net_vol_sum = round(sum(df['NET_VOLUME']),2)
#     # revenue_sum = round(sum(df['REVENUE']),2)
#     # cn_summary = {'COUNTRY' : 'CN', 'VOLUME': vol_sum, "NET VOLUME": net_vol_sum, "REVENUE" : revenue_sum, 'TIME': Get_time_String()}
#     # print(cn_summary)
#     # return json.dumps([bar, cn_pnl_bar, cn_heat_map, cn_summary], cls=plotly.utils.PlotlyJSONEncoder)
#
#     return get_country_charts(country="CN", df= get_cn_df())
#
#
#
# @analysis.route('/analysis/tw_live_vol_ajax')
# @roles_required()
# # Gets the cn df, and uses it to plot the various charts.
# def tw_live_vol_ajax():
#     return get_country_charts(country="TW", df= get_tw_df())

# # Generic get the country charts.
# def get_country_charts(country, df):
#
#     start = datetime.datetime.now()
#     bar = plot_open_position_net(df, chart_title = "[{}] Open Position".format(country))
#     pnl_bar = plot_open_position_revenue(df, chart_title="[{}] Open Position Revenue".format(country))
#     heat_map = plot_volVSgroup_heat_map(df,chart_title="[{}] Net Position by Group".format(country))
#     #print("Getting {} df and charts {} Seconds.".format(country,(datetime.datetime.now()-start).total_seconds()))
#     vol_sum = '{:,.2f}'.format(round(sum(df['VOLUME']),2))
#     revenue_sum = '{:,.2f}'.format(round(sum(df['REVENUE']),2))
#     summary = {'COUNTRY' : country, 'VOLUME': vol_sum, 'REVENUE' : revenue_sum, 'TIME': Get_time_String()}
#     #print(cn_summary)
#     return json.dumps([bar, pnl_bar, heat_map, summary], cls=plotly.utils.PlotlyJSONEncoder)

# Trying to catch players that are Scalping with specific comments
@analysis.route('/Client_Comment_Scalp', methods=['GET', 'POST'])
@roles_required()
def Client_Comment_Scalp():

    title = "Catching Client Comment"
    header = "Catching Client Comment"

    description = Markup("Catching Client Comment that are like %-%=%")


        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html",
                           backgroud_Filename=background_pic("Client_Comment_Scalp"),
                           icon= "css/Globe.png", Table_name="Scalpers", \
                           title=title,
                           ajax_url=url_for('analysis.Client_Comment_Scalp_ajax', _external=True),
                            header=header, setinterval=15,
                           description=description, replace_words=Markup(['(Client Side)']))



@analysis.route('/Client_Comment_Scalpajax', methods=['GET', 'POST'])
@roles_required()
def Client_Comment_Scalp_ajax():


    if not cfh_fix_timing():
        return json.dumps([{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}])

    min_backtrace = 60*2
    # Want to reduce the overheads.
    server_time_diff_str = " {} ".format(session["live1_sgt_time_diff"]) if "live1_sgt_time_diff" in session else \
            "SELECT RESULT FROM aaron.`aaron_misc_data` where item = 'live1_time_diff'"


    sql_statement = """SELECT LOGIN, TICKET, OPEN_TIME, CLOSE_TIME, VOLUME * 0.01 as LOTS, 
                        mt4_trades.SYMBOL, SWAPS, PROFIT, `COMMENT`, `GROUP`, symbol_rebate.REBATE * VOLUME * 0.01 as REBATE
            FROM live1.mt4_trades, live1.symbol_rebate
            WHERE `comment` like '%-%=%'
            and mt4_trades.SYMBOL = symbol_rebate.SYMBOL 
            and OPEN_TIME >= NOW()-INTERVAL ({ServerTimeDiff_Query}) HOUR - INTERVAL {min_backtrace} MINUTE
            AND TICKET NOT in (SELECT TICKET FROM aaron.cn_scalp_data)
            """.format(ServerTimeDiff_Query=server_time_diff_str, min_backtrace = min_backtrace)

    sql_query = text(sql_statement)
    return_val = query_SQL_return_record(sql_query)

    if len(return_val) == 0:
        return_val = [{"Comment":"No Clients Found"}]
    else:

        df = pd.DataFrame(return_val)
        df["OPEN_TIME"] = df["OPEN_TIME"].apply(lambda x: "{}".format(x))
        df["CLOSE_TIME"] = df["CLOSE_TIME"].apply(lambda x: "{}".format(x))
        df["ALERT_TIME"] ="NOW()"   # Want to force this column to input into SQL
        df["ALERT_TIME"] = "NOW()"  # Want to force this column to input into SQL


        col_needed = ["LOGIN", "SYMBOL", "COMMENT", "PROFIT", "REBATE"]

        df2 = df.copy() # Make a Copy.
        df2["LOGIN"] = df2["LOGIN"].apply(lambda x: live_login_analysis_url_External(Live=1, Login=x))
        data_dict  = df2[col_needed].to_dict('r')
        data_list = [list(d.values()) for d in data_dict]   # Get the values
        data_list_2 = [ " | ".join(["{}".format(str(e)) for e in d]) for d in data_list]    # convert to str, add '
        print(data_list_2)


        #all_login = ["{}".format(x) for x in list(df["LOGIN"])]

        # Need to alert Risk
        async_Post_To_Telegram(AARON_BOT, "Scalpers (Live 1) With specific Comment.\n{table_col}\n{data}".format( table_col= " | ".join(col_needed), data="\n".join(data_list_2)),
            TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)

        # async_Post_To_Telegram(AARON_BOT, '<a href="http://www.google.com/">inline URL</a> | Together with something else\nHello',
        #                        TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)


        # Will check with config file if email flag is True or False
        email_flag, email_recipients = email_flag_fun("scalping_comment")
        if email_flag:
        #EMAIL_LIST_BGI
            async_send_email(To_recipients=email_recipients, cc_recipients=[],
                             Subject="Live 1 Bonus Scalpers",
                             HTML_Text="""{Email_Header}Hi,<br><br>Clients from Live 1 are hitting {sym}<br>{table}<br>
                                   <br><br>This Email was generated at: {datetime_now} (SGT)<br><br>Thanks,<br>Aaron{Email_Footer}""".format(
                                 Email_Header=Email_Header, sym=" ,".join(list(df["SYMBOL"].unique())),
                                 table = Array_To_HTML_Table(Table_Header = col_needed,Table_Data=data_list),
                                 datetime_now=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                 Email_Footer=Email_Footer), Attachment_Name=[])



        sql_col_needed = ["TICKET", "LOGIN", "SYMBOL", "COMMENT", "ALERT_TIME"]
        sql_data_dict  = df[sql_col_needed].to_dict('r')

        sql_data_list = [list(d.values()) for d in sql_data_dict]   # Get the values
        sql_data_list_2 = [ ["'{}'".format(str(e)) if e!= "NOW()" else e for e in d] for d in sql_data_list]    # convert to str, add '
        data_to_insert = [" ({}) ".format(" , ".join(d)) for d in sql_data_list_2]     # Want to insert to SQL

        # inset into SQL
        async_sql_insert(app=current_app._get_current_object(),
                        header="INSERT INTO aaron.CN_SCALP_Data (TICKET,LOGIN, SYMBOL, `COMMENT`, `ALERT_TIME`) VALUES ",
                        values = data_to_insert,
                        footer = " ON DUPLICATE KEY UPDATE SYMBOL=VALUES(SYMBOL)")

    return json.dumps(return_val, cls=plotly.utils.PlotlyJSONEncoder)


def plot_open_position_net(df, chart_title):

    # Trying to get core symbol
    df['SYMBOL'] = df['SYMBOL'].apply(lambda x: x[:6])


    df['ABS_NET_VOLUME'] = abs(df['NET_VOLUME'])  # We want to look at the abs value. (Dosn't matter long or short)

    # Want to do the Group, to start having a consolidated df to plot.
    df_sum = df.groupby('SYMBOL')[['VOLUME', 'NET_VOLUME', 'ABS_NET_VOLUME']].sum().reset_index().sort_values('ABS_NET_VOLUME', ascending=True)


    top_n_symbols = 15  # Want to see top how many?
    topn_symbol_list = list(df_sum.tail(top_n_symbols)['SYMBOL'].to_dict().values())
    df_sum_top_n = df_sum[df_sum['SYMBOL'].isin(topn_symbol_list)]

    vol_x = df_sum_top_n['VOLUME']
    net_vol_x = df_sum_top_n['NET_VOLUME']
    symbols = df_sum_top_n['SYMBOL']
    fig = go.Figure(data=[
        go.Bar(name="Total Volume", y=symbols, x=vol_x, orientation='h', text=vol_x, textposition='auto',
               cliponaxis=False, textfont=dict(size=14)),
        go.Bar(name="Net Volume", y=symbols, x=net_vol_x, orientation='h', text=net_vol_x, textposition='auto',
               cliponaxis=False, textfont=dict(size=14))
    ])

    # Change the bar mode
    fig.update_layout(barmode='group')
    fig.update_layout(
        autosize=False,
        width=420,
        height=800,
        margin=dict( pad=10),
        yaxis=dict(
            title_text="Symbols",
            titlefont=dict(size=20),
            ticks="outside", tickcolor='white', ticklen=15,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Volume",
            titlefont=dict(size=20),layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text='{} (Client Side)'.format(chart_title),
        titlefont=dict(size=20),
        title_x=0.5
    )
    # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #
    # return graphJSON
    return fig

# PLot the historical data of the symbol
# Either by Revenue, or by Volume.
def plot_symbol_history(df, by, chart_title, color_by, group_by = []):
    # Lots was saved as decimal. Need to convert to float
    df[by.upper()] = df[by.upper()].apply(float)

    df_country = df.groupby(group_by).sum().reset_index()
    fig = px.bar(df_country, x='DATE', y=by.upper(), color=color_by)
    #fig.show()

    # Change the bar layout
    fig.update_layout(
        autosize=True,
        width=750,
        height=500,
        margin=dict(pad=10),
        yaxis=dict(
            title_text="Total {}".format(by),
            titlefont=dict(size=20),
            ticks="outside", tickcolor='white', ticklen=15,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="History Daily {}".format(by),
            titlefont=dict(size=20), layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text=chart_title,
        titlefont=dict(size=20),
        title_x=0.5
    )
    # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #
    # return graphJSON
    return fig



def plot_symbol_opentime(df, chart_title):

    # Lots was saved as decimal. Need to convert to float
    df["LOTS"] = df["LOTS"].apply(float)

    #fd_5min = df.groupby(["COUNTRY"]).resample('5T', on='OPEN_TIME').sum().reset_index()
    fd_5min = df.groupby(["COUNTRY"]).resample('15T', on='OPEN_TIME').sum().reset_index()

    #print(fd_5min)
    fig = px.bar(fd_5min, x='OPEN_TIME', y='LOTS', color="COUNTRY", template="ggplot2",
                 color_discrete_sequence=px.colors.qualitative.Alphabet, )



    #fig.show()
    #
    # Change the bar layout
    fig.update_layout(
        autosize=True,
        #width=750,
        #height=500,
        margin=dict( pad=10),
        yaxis=dict(
            title_text="Total Lots",
            titlefont=dict(size=20),
            ticks="outside", tickcolor='white', ticklen=15,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Open Time (Live 1 Time)",
            titlefont=dict(size=20),layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text=chart_title,
        titlefont=dict(size=20),
        title_x=0.5,
        font=dict(
            family="Montserrat, monospace",
            size=18,
            color="Black"
        )
    )
    # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #
    # return graphJSON
    return fig


# Want to plot how the position looks for the past few hours/days
# Snap shot graph.
def plot_symbol_book_total(df, chart_title):

    if 'DATETIME' not in df:
        return []

    # Sort by datetime, to be able to plot it.
    df.sort_values(by=['DATETIME'], inplace=True)

    print(df)

    if "COUNTRY" in df:
        df_t2 = df.groupby(["COUNTRY", "DATETIME"]).sum().reset_index()
        fig = px.line(df_t2, x='DATETIME', y='FLOATING_VOLUME', color='COUNTRY', template="ggplot2",
                      color_discrete_sequence=px.colors.qualitative.Alphabet,)
    elif "SYMBOL" in df:
        df_t2 = df.groupby(["SYMBOL", "DATETIME"]).sum().reset_index()
        fig = px.line(df_t2, x='DATETIME', y='FLOATING_VOLUME', color='SYMBOL',  template="ggplot2",
                      color_discrete_sequence=px.colors.qualitative.Alphabet,)
    else:
        return []

    # Chart layout
    fig.update_layout(
        autosize=True,
        #width=700,
        #height=500,
        margin=dict(pad=10),
        yaxis=dict(
            title_text="Floating Lots",
            titlefont=dict(size=20),
            ticks="outside", tickcolor='white', ticklen=15,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Datetime (Server - Live 1)",
            titlefont=dict(size=20),layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text=chart_title,
        titlefont=dict(size=20),
        title_x=0.5,
        font=dict(
            family="Montserrat, monospace",
            size=18,
            color="Black"
        )
    )


    # hide weekends
    # fig.update_xaxes(
    #     rangebreaks=[
    #         dict(bounds=["sat", "mon"])  # hide weekends
    #     ]
    # )

    #fig.show()
    return fig



def plot_open_position_revenue(df, chart_title):
    # Trying to get core symbol
    df['SYMBOL'] = df['SYMBOL'].apply(lambda x: x[:6])

    # Trying to get revenue on BGI Side
    df['REVENUE'] = -1 * df['REVENUE']  # Want to flip, for our View

    df_PnL = df.groupby('SYMBOL')[['REVENUE']].sum().reset_index().sort_values('REVENUE', ascending=True)
    top_n_symbols = 100  # Want to see top how many?
    topn_symbol_list = list(df_PnL.tail(top_n_symbols)['SYMBOL'].to_dict().values())
    df_sum_top_n = df_PnL[df_PnL['SYMBOL'].isin(topn_symbol_list)]

    pnl_x = df_sum_top_n['REVENUE'].round(2)
    pnl_text = pnl_x.apply(lambda x: text_numbers(x))  # for labelling
    pnl_color = pnl_x.apply(lambda x: "green" if x >= 0 else 'red')

    symbols = df_sum_top_n['SYMBOL']

    fig = go.Figure(data=[
        go.Bar(name="Total Volume", y=symbols, x=pnl_x, orientation='h', text=pnl_text, textposition='auto',
               cliponaxis=False,
               marker_color=pnl_color,
               textfont=dict(
                   size=14,
               ))
    ])

    # Change the bar mode
    fig.update_layout(barmode='group')

    # Figure Layout.
    fig.update_layout(
        autosize=False,
        width=420,
        height=800,
        yaxis=dict(
            title_text="Symbols",
            titlefont=dict(size=20),
            automargin=True,
            ticks="outside", tickcolor='white', ticklen=50,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Revenue",
            titlefont=dict(size=20),
            automargin=True,
            layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text='{} (BGI Side)'.format(chart_title),
        titlefont=dict(size=20),
        title_x=0.5,
        margin=dict(
            pad=10)
    )

    fig.update_yaxes(automargin=True)
    #graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #return graphJSON
    return fig

@analysis.route('/analysis/plotly_index')
@roles_required()
def cn_index():
    return render_template('index_plotly.html', title="Float Country")



def plot_volVSgroup_heat_map(df, chart_title):

    df_group_volume = df.groupby(['SYMBOL', 'GROUP'])['NET_VOLUME'].sum().reset_index().sort_values('GROUP',
                                                                                                    ascending=True)
    df_heat_map_data = df_group_volume.pivot(index='SYMBOL', columns='GROUP')[['NET_VOLUME']]

    z = [list([float(j) for j in df_heat_map_data.iloc[i].values]) for i in range(len(df_heat_map_data))]
    y = list(df_heat_map_data.index)
    x = list(x[1] for x in df_heat_map_data.keys())

    fig = go.Figure(data=go.Heatmap(
        z=z, x=x, y=y,
        hoverongaps=False, colorscale='Viridis', reversescale=True))

    fig.update_layout(
        autosize=False,
        width=420,
        height=800,
        yaxis=dict(
            title_text="Symbols",
            titlefont=dict(size=20),
            automargin=True,
            ticks="outside", tickcolor='white', ticklen=5,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Client Group",
            titlefont=dict(size=20),
            automargin=True,
            layer='below traces'
        ),
        xaxis_tickfont_size=10,
        title_text='{} (Client Side)'.format(chart_title),
        titlefont=dict(size=20),
        title_x=0.5
    )

    fig.update_yaxes(automargin=True)
    # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    # return graphJSON
    return fig


    # df_group_volume = df.groupby(['SYMBOL', 'GROUP'])['VOLUME'].sum().reset_index().sort_values('GROUP', ascending=True)
    # df_heat_map_data = df_group_volume.pivot(index='SYMBOL', columns='GROUP')[['VOLUME']].fillna(0)
    #
    # z = [list([float(j) for j in df_heat_map_data.iloc[i].values]) for i in range(len(df_heat_map_data))]
    # y = list(df_heat_map_data.index)
    # x = list(df_heat_map_data.keys())
    #
    # fig = go.Figure(data=go.Heatmap(
    #     z = z, x = x, y=y,
    #     hoverongaps=False))
    # fig.show()


# Used for account details of client.
# To show (something like tableau, for people with no access)
def plot_account_details(account_details):

    x = ['Deposit', 'Withdrawal', 'Profits']
    y = [account_details[0]["DEPOSIT"], -1 * account_details[0]["WITHDRAWAL"], round(account_details[0]["CLIENT PROFIT"],2)]

    # Use the hovertext kw argument for hover text
    fig = go.Figure(data=[go.Bar(x=x, y=y,
                                 hovertext=['Deposit', 'Withdrawal', 'Profits'], text=y, textposition='auto')])
    # Customize aspect
    fig.update_traces(marker_color='rgb(158,202,225)', marker_line_color='rgb(8,48,107)',
                      marker_line_width=1.5, opacity=0.6)
    fig.update_layout(title_text='Total Profits % Calculation',
                      yaxis=dict(
                          title_text="Amount",
                          titlefont=dict(size=20),
                          automargin=True,
                          ticks="outside", tickcolor='white', ticklen=5,
                          layer='below traces'
                      ),
                      yaxis_tickfont_size=14,
                      xaxis=dict(
                          title_text="",
                          titlefont=dict(size=20),
                          automargin=True,
                          layer='below traces'
                      ),
                      xaxis_tickfont_size=10,
                      autosize=True,
                      width=700,
                      height=500,
                      title_x=0.5,  titlefont=dict(size=20),
                      )
    return fig



# Used for account details of client.
# To show (something like tableau, for people with no access)
def plot_symbol_tradetime_duration(df_data):



    fig = go.Figure(data=[go.Bar(x=df_data.SYMBOL, y=df_data.DURATION, text=df_data.DURATION, textposition='auto')])
    # Customize aspect
    fig.update_traces(marker_color='rgb(158,202,225)', marker_line_color='rgb(8,48,107)',
                      marker_line_width=1.5, opacity=0.6)

    fig.update_layout(title_text='Trade Duration Average (Only for above trades)',
                      yaxis=dict(
                          title_text="Time (s)",
                          titlefont=dict(size=20),
                          automargin=True,
                          ticks="outside", tickcolor='white', ticklen=5,
                          layer='below traces'
                      ),
                      yaxis_tickfont_size=14,
                      xaxis=dict(
                          title_text="Symbol",
                          titlefont=dict(size=20),
                          automargin=True,
                          layer='below traces'
                      ),
                      xaxis_tickfont_size=10,
                      autosize=True,
                      width=700,
                      height=500,
                      title_x=0.5,  titlefont=dict(size=20),
                      )
    return fig

# Used for account details of client.
# To show (something like tableau, for people with no access)
def plot_symbol_tradetime_duration_2(df_data):

    # Want to get the duration string if it has not been computed
    if "DURATION_STR" not in df_data:
        df_data["DURATION_STR"] = df_data.apply(lambda x: trade_duration_bin((x["CLOSE_TIME"] - x["OPEN_TIME"]).total_seconds()), axis=1)
    df_data_2 = df_data.groupby(by=["SYMBOL", "DURATION_STR"]).count().reset_index()


    fig = px.bar(df_data_2, x="DURATION_STR", y="TICKET", color="SYMBOL", text="SYMBOL")

    # Customize aspect
    fig.update_traces(marker_line_color='rgb(8,48,107)', marker_line_width=1.5, opacity=0.8)

    fig.update_layout(title_text='Trade Duration Average (Only for above trades)',
                      yaxis=dict(
                          title_text="Count",
                          titlefont=dict(size=20),
                          automargin=True,
                          ticks="outside", tickcolor='white', ticklen=5,
                          layer='below traces'
                      ),
                      yaxis_tickfont_size=14,
                      xaxis=dict(
                          title_text="Duration",
                          titlefont=dict(size=20),
                          automargin=True,
                          layer='below traces',
                          categoryorder='array',
                          categoryarray=["<= 1 min", "1-2 mins", "2-3 mins", "3-5 mins",
                                         "5 mins - 10 Mins", "10 mins - 1 hour",
                                         "> 1 Hour"]
                      ),

                      xaxis_tickfont_size=10,
                      autosize=True,
                      width=700,
                      height=500,
                      title_x=0.5, titlefont=dict(size=20),
                      )

    # fig.show()
    return fig



# To view Client's trades as well as some simple details.
@analysis.route('/Client_Details_form', methods=['GET', 'POST'])
@roles_required()
def Client_trades_form(Live="", Login=""):
    title = "Client Details Form"
    header = "Client Details Form"
    description = Markup("Will be able to see client's details as well as open trades as well as some recent trades.")
    form = Live_Login()
    # file_form = File_Form()

    if request.method == 'POST' and form.validate_on_submit():

        Live = form.Live.data  # Get the Data.
        Login = form.Login.data
        # Want to redirect this to some other pages.
        return redirect(url_for('analysis.Client_trades_Analysis', Live=Live, Login=Login))


    return render_template("General_Form.html",
                           backgroud_Filename=background_pic("Client_trades_form"),
                           title=title,
                           header=header,
                           form=form, no_backgroud_Cover=True,
                           description=description)


# To Have a look at the client's trades and details
# Will query from SQL and display on screen.
@analysis.route('/Client_Trades/<Live>/<Login>', methods=['GET', 'POST'])
@roles_required()
def Client_trades_Analysis(Live="", Login=""):

    title = "Live:{Live}, Account:{Login} Trades".format(Live=Live, Login=Login)
    header  = "Live:{Live}, Account:{Login} Trades".format(Live=Live, Login=Login)

    description = Markup("Live:{Live}, Account:{Login} Trades<br>Information from SQL DB<br>Information are on client side.<br>Limited to 100 close trades.<br>Shows all open trades".format(Live=Live, Login=Login))

    if Live == "" or Login == "":   # There are no information.
        flash("There were no Live or Login details.")
        return redirect("main_app.index")

    tableau_url = individual_client_analysis(live=Live, login=Login)

    # Table names will need be in a dict, identifying if the table should be horizontal or vertical.
    # Will try to do smaller vertical table to put 2 or 3 tables in a row.


    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("Client_trades_Analysis"), icon="",
                           Table_name={"Live: {}, Login: {}".format(Live, Login):"V1",
                                       "Profit Calculation":"V2",
                                       "Net Position": "H1",
                                       "Open Trades": "H2",
                                       "Past Trades" : "H3",
                                       "Deposit/Withdrawal plot":"P1",
                                       "Average Trade Timings":"P2"},
                           title=title,
                           ajax_url=url_for('analysis.Client_trades_Analysis_ajax',_external=True, Live=Live, Login=Login),
                           header=header,
                           description=description, no_backgroud_Cover=True,
                           tableau_url=tableau_url,
                           replace_words=Markup(["Today"]))

# To remove the account from being excluded.
@analysis.route('/Client_Trades_ajax/<Live>/<Login>', methods=['GET', 'POST'])
@roles_required()
def Client_trades_Analysis_ajax(Live="", Login=""):


    if Live not in ["1","2","3","5"] or Login == "":   # There are no information.
        return json.dumps({"H1": [{'Results': 'Error in Live/Login'}]})

    # First Query. Want to get the MT4 Group, Loginm Currency, Margin call etc...
    sql_statement = """SELECT LOGIN, mt4_users.`GROUP`,mt4_groups.CURRENCY,mt4_groups.MARGIN_CALL, mt4_groups.MARGIN_STOPOUT, 
                    mt4_users.`ENABLE`, ENABLE_READONLY, `NAME`, LEVERAGE,
                        ROUND(BALANCE,2) as BALANCE, ROUND(mt4_users.CREDIT , 2) as CREDIT,
                        ROUND(EQUITY, 2) as EQUITY, ROUND(MARGIN, 2) as `MARGIN`, 
                        ROUND(MARGIN_LEVEL,2) as 'MARGIN_LEVEL  (E/M)', ROUND(MARGIN_FREE, 2) as MARGIN_FREE
            FROM live{Live}.mt4_users , live{Live}.mt4_groups 
            WHERE `Login`='{Login}' AND mt4_groups.`GROUP` = mt4_users.`GROUP` """.format(Live=Live, Login=Login)

    #print(sql_statement)
    sql_statement = sql_statement.replace("\n", "").replace("\t", "")
    login_details = Query_SQL_db_engine(sql_statement)

    # Want to print it propely, with the approperiate commas
    ignore_col = ["LOGIN", "GROUP", "CURRENCY", "MARGIN_CALL", "MARGIN_STOPOUT", "ENABLE", "ENABLE_READONLY", "BALANCE"]
    login_details = [{k:"{:,.2f}".format(d) if k not in ignore_col and isfloat(d) else d for k,d in l.items()} for l in login_details]

    print(login_details)

    if len(login_details) <= 0:   # There are no information.
        #print("No Login Details.")
        return json.dumps({"H1": [{'Results': 'No Login/Live'}]})

    # Color the background for Balance to highlight it.
    # login_details[0]["BALANCE"] = "<span style = 'background-color:#4af076;' >{}</span> ".format(login_details[0]["BALANCE"])
    login_details[0]["BALANCE"] = profit_red_green(login_details[0]["BALANCE"])
    login_details[0]["GROUP"] = client_group_url(login_details[0]["GROUP"])


    # # Write the SQL Statement and Update to disable the Account monitoring.
    # # Want the CLOSE TRADES limited to 100
    # # AND all the OPEN trades
    sql_statement = """(SELECT TICKET,  mt4_trades.SYMBOL, VOLUME * 0.01 AS LOTS, CMD, OPEN_TIME, 
		CLOSE_TIME, SWAPS, PROFIT, `COMMENT`, `GROUP`,COALESCE(REBATE* VOLUME * 0.01,0) as REBATE
        FROM live{Live}.mt4_trades  LEFT JOIN live{Live}.symbol_rebate ON mt4_trades.SYMBOL = symbol_rebate.SYMBOL
        WHERE `Login`='{Login}' AND CLOSE_TIME = "1970-01-01 00:00:00" AND CMD < 2)
        
        UNION
        
        (SELECT TICKET, mt4_trades.SYMBOL, VOLUME * 0.01 AS LOTS, CMD, OPEN_TIME, 
                CLOSE_TIME, SWAPS, PROFIT, `COMMENT`, `GROUP`, COALESCE(REBATE* VOLUME * 0.01,0)  as REBATE
	FROM live{Live}.mt4_trades LEFT JOIN live{Live}.symbol_rebate ON mt4_trades.SYMBOL = symbol_rebate.SYMBOL
	WHERE `Login`='{Login}' and CLOSE_TIME <> "1970-01-01 00:00:00"  AND CMD < 2
	ORDER BY CLOSE_TIME DESC
	LIMIT 100 )""".format(Live=Live, Login=Login)

    sql_statement = sql_statement.replace("\n", " ").replace("\t", " ")
    result = Query_SQL_db_engine(sql_statement)
    df_data = pd.DataFrame(result)

    #
    # # symbol_average_tradetime = Average_trade_time_per_symbol(df_data)
    #
    # # average_trade_duration_fig = plot_symbol_tradetime_duration(symbol_average_tradetime)




    # Want to get the live 1 to SGT time difference.
    # Since server is running on SGT (GMT + 8) Time.
    if not "live1_sgt_time_diff" in session:
        session["live1_sgt_time_diff"] = get_live1_time_difference()
        print("Getting live1_sgt_time_diff from SQL")

    # Get net positions for all.
    net_position = Calculate_Net_position(df_data)
    net_position_dict = net_position.to_dict("record")
    net_position_dict_clean = [{k: "{}".format(d) for k, d in r.items()} for r in net_position_dict]

    # Want to get total deposit, withdrawal.. etc.
    Sum_details = Sum_total_account_details(Live, Login)    # returns a list of 1 dict.
    deposit_withdrawal_fig = plot_account_details(Sum_details)   # To get the figure to show.

    # Want to tidy up the numbers, to be expressed in commas.
    col_to_add_commas = ['WITHDRAWAL', 'LOTS', '% PROFIT']
    for c in col_to_add_commas:
        if c in Sum_details[0] and type(Sum_details[0][c]) == float:
            Sum_details[0][c] = "{:,.2f}".format(Sum_details[0][c])

    # Want to add some colors to the string.
    Sum_details[0]["DEPOSIT"] = '<span style="color:{Color}">${value:,.2f}</span>'.format( \
                                                            Color=color_negative_red(Sum_details[0]["DEPOSIT"] ), \
                                                            value=Sum_details[0]["DEPOSIT"] )
    Sum_details[0]["FLOATING PROFIT"] =  '<span style="color:{Color}">${value:,.2f}</span>'.format( \
                                                            Color=color_negative_red(Sum_details[0]["FLOATING PROFIT"] ), \
                                                            value=Sum_details[0]["FLOATING PROFIT"] )
    Sum_details[0]["CLIENT PROFIT"] =  '<span style="color:{Color}">${value:,.2f}</span>'.format( \
                                                            Color=color_negative_red(Sum_details[0]["CLIENT PROFIT"] ), \
                                                            value=Sum_details[0]["CLIENT PROFIT"] )
    if isfloat(Sum_details[0]["PER LOT AVERAGE"]):
        Sum_details[0]["PER LOT AVERAGE"] =  '<span style="color:{Color}">${value:,.2f}</span>'.format( \
                                                            Color=color_negative_red(Sum_details[0]["PER LOT AVERAGE"] ), \
                                                            value=Sum_details[0]["PER LOT AVERAGE"] )




    if "PROFIT" in df_data:     # Color the profit
        df_data["PROFIT"] =  df_data["PROFIT"].apply(profit_red_green) # Print in 2 D.P,with color (HTML))

    if "SWAPS" in df_data:
        df_data["SWAPS"] =  df_data["SWAPS"].apply(profit_red_green)

    # Want to calculate the time duration. Let format deal with printing it.
    # If the trade is closed, we want to find out how long it was opened for.
    # If it's still opened, we want to know how long..
    # We don't care about the BALANCE, CREDIT, and the sell/buy stop/limit

    # TODO: Should take into account server time - open_time
    df_data["DURATION"] = df_data.apply(lambda x: "-" if int(x['CMD']) >= 2 else \
                        (x["CLOSE_TIME"] - x["OPEN_TIME"] \
                        if x["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00') else \
                        pd.Timestamp.now() - datetime.timedelta(hours = float(session["live1_sgt_time_diff"])) - x["OPEN_TIME"]), axis=1)

    if "DURATION" in df_data:
        # Want to get the duration string
        #df_data["DURATION_SEC"] = df_data["DURATION"].apply(lambda x: x.total_seconds() if isinstance(x, datetime.timedelta) else x)
        df_data["DURATION_STR"] =  df_data["DURATION"].apply(lambda x: trade_duration_bin(x.total_seconds()) \
                            if isinstance(x, datetime.timedelta) else x)
    else:
        df_data["DURATION_STR"] = ""

    # Plot the graph. Would e best to use the df_data["DURATION"] that was already calculated.
    # Want only CLOSED trades.
    #print(df_data)
    df_data_closed = df_data[df_data["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00')]
    if len(df_data_closed) > 0:
        average_trade_duration_fig = plot_symbol_tradetime_duration_2(df_data[df_data["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00')])
    else:
        average_trade_duration_fig = []
    df_data_closed = None

    # Want to see which trades are below 3 mins..
    df_data["DURATION"] =  df_data["DURATION"].apply(lambda x: '<span style="color:red">{value}</span>'.format(value=x)  \
                            if isinstance(x, datetime.timedelta) and x < datetime.timedelta(seconds=180) else x )



    cmd = {0: "BUY", 1: "SELL", 2: "BUY LIMIT", 3:"SELL LIMIT", 4: "BUY STOP", 5: "SELL STOP", 6: "BALANCE", 7: "CREDIT"}

    if "CMD" in df_data:
        df_data["CMD"] = df_data["CMD"].apply(lambda x: cmd[x] if x in cmd else x)

    # df_data["DURATION_SEC"] = df_data["DURATION"] .apply(lambda x: x.total_seconds())

    # Re-arrange the index
    df_data = df_data[[ 'TICKET', 'SYMBOL', 'CMD','LOTS', 'OPEN_TIME',
                        'CLOSE_TIME',"DURATION",'SWAPS', 'PROFIT', 'REBATE' ,'GROUP',
                        'COMMENT', 'DURATION_STR']]

    # Want to write in the Client Group URL
    df_data["GROUP"] =  df_data["GROUP"].apply(client_group_url)

    # Want to get the open trades.
    open_position =  df_data[df_data["CLOSE_TIME"] == pd.Timestamp('1970-01-01 00:00:00')]  # Only open trades.
    open_position["OPEN_TIME"] = open_position["OPEN_TIME"].apply(lambda x: "{}".format(x)) # Format OPEN_TIME to be something printable.
    open_position.drop(columns=['CLOSE_TIME', 'DURATION'], inplace=True)
    open_position_dict = open_position.to_dict("record")



    # Overwrite df_data to consist of only the closed trades
    df_data = df_data[df_data["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00')]  # Only open trades

    # Sort by Close time. Descending.
    df_data.sort_values(by=["CLOSE_TIME", "DURATION"], ascending=False, inplace=True)

    # To make it JSON printable
    df_data["DURATION"] = df_data["DURATION"].apply(lambda x: "{}".format(x))

    # Want it to be printable to JSON
    df_data["CLOSE_TIME"] = df_data["CLOSE_TIME"].apply(lambda x: "{}".format(x))
    df_data["OPEN_TIME"] = df_data["OPEN_TIME"].apply(lambda x: "{}".format(x))

    closed_trades = df_data.to_dict("record")


    #print(df_data.to_html())
    return_html = df_data.to_html(table_id ="Data_table_Div1_table", index=False, \
                    classes=["table", "compact", "row-border", "table-hover", "table-sm", "table-responsive-sm", "basic_table", "bg-light", "dataTable", "no-footer"],
                                  escape=False,
                                  border =0).replace("\n", "")

    # Want to input the caption into the HTML data.
    thead_index = return_html.find("<thead>")
    caption = "Live: {Live}, Login:{Login}".format(Live=Live, Login=Login)
    return_html = return_html[:thead_index] + "<caption>{}</caption>".format(caption) + return_html[thead_index:]

    #print(net_position)

    # Want to make the data printable
    #result_clean = [{k : "{}".format(d) for k,d in r.items()} for r in result]
    # #return json.dumps(return_html)

    print(net_position_dict_clean)
    # Return "Trades" and "Net position"
    return json.dumps({"V1" : login_details, "V2": Sum_details, "H1": net_position_dict_clean,
                       "H2" : open_position_dict,
                       "H3": closed_trades,
                        "P1":deposit_withdrawal_fig,
                       "P2": average_trade_duration_fig}, cls=plotly.utils.PlotlyJSONEncoder)


@analysis.route('/symbol/popup')
@login_required
def symbol_popup():

    return render_template('popup.html')

#
# @analysis.route('/mt5_test', methods=['GET', 'POST'])
# @roles_required()
# def mt5_test():
#
#     title = "Symbol Float"
#     header = "Symbol Float"
#
#     query = mt5_BBook_select_insert()
#
#     result_data = SQL_insert_MT5_statement(query)
#
#     # sql_insert = """INSERT INTO aaron.`bgi_mt5_float_save` (entity, symbol, net_floating_volume, floating_volume, floating_revenue, datetime, closed_revenue_today, closed_vol_today)
#     # VALUES ("SG", "EURUSD", "1", "1.02", "2.02", now(), "2.34", "1.234")"""
#     #
#     # SQL_insert_MT5_statement(sql_insert)
#     #
#     # print(pd.DataFrame(result_data))
#
#
#     return "Hello<br><br>testing 1 2 3 <br>Thanks!"






