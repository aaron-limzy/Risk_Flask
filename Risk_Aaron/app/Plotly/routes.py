from flask import Blueprint, render_template, Markup, url_for, request, session, current_app, flash, redirect
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from app.Swaps.A_Utils import *
from sqlalchemy import text
from app.extensions import db, excel

import plotly
import plotly.graph_objs as go
import plotly.express as px

import pandas as pd
import numpy as np
import json

from app.decorators import roles_required
from app.Plotly.forms import  Live_Login
#from app.Plotly.table import Client_Trade_Table

import emoji
import flag

from app.Swaps.forms import UploadForm

from flask_table import create_table, Col

import requests

import pyexcel
from werkzeug.utils import secure_filename

from Helper_Flask_Lib import *

import decimal

analysis = Blueprint('analysis', __name__)


AARON_BOT = "736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0"		# For Mismatch and LP Margin
TELE_ID_USDTWF_MISMATCH = "776609726:AAHVrhEffiJ4yWTn1nw0ZBcYttkyY0tuN0s"        # For USDTWF
TELE_CLIENT_ID = ["486797751"]        # Aaron's Telegram ID.

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



@analysis.route('/Save_BGI_Float', methods=['GET', 'POST'])
@roles_required()
def save_BGI_float():

    title = "Save BGI Float"
    header = "Saving BGI Float"

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
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/city_overview.jpg', icon="css/save_Filled.png",
                           Table_name="Save Floating 💾", title=title,
                            ajax_url=url_for('analysis.save_BGI_float_Ajax', _external=True), header=header, setinterval=12,
                           description=description, replace_words=Markup(["Today"]))


# Insert into aaron.BGI_Float_History_Save
# Will select, pull it out into Python, before inserting it into the table.
# Tried doing Insert.. Select. But there was desdlock situation..
@analysis.route('/save_BGI_float_Ajax', methods=['GET', 'POST'])
@roles_required()
def save_BGI_float_Ajax(update_tool_time=1):

    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([{'Update time': "Not updating, as Market isn't opened. {}".format(Get_time_String())}])

    if check_session_live1_timing() == False:    # If False, It needs an update.
        Post_To_Telegram(AARON_BOT, "Retrieving and saving previous day PnL.", TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)
        # TOREMOVE: Comment out the print.
        print("Saving Previous Day PnL")
        #TODO: Maybe make this async?
        save_previous_day_PnL()                 # We will take this chance to get the Previous day's PnL as well.

    # Want to reduce the query overheads. So try to use the saved value as much as possible.
    server_time_diff_str = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session \
                else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"

    sql_statement = """SELECT LIVE, COUNTRY, SYMBOL1 as SYMBOL, NET_FLOATING_VOLUME, SUM_FLOATING_VOLUME, FLOATING_PROFIT*-1 AS FLOATING_REVENUE,SUM_CLOSED_VOLUME, -1*CLOSED_PROFIT, DATE_SUB(now(),INTERVAL ({ServerTimeDiff_Query}) HOUR) as DATETIME FROM(
    (SELECT 'live1' AS LIVE,group_table.COUNTRY, 
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_FLOATING_VOLUME,
	SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_CLOSED_VOLUME,
		SUM(CASE WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 0) THEN mt4_trades.VOLUME WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 1) THEN -1*mt4_trades.VOLUME ELSE 0 END)*0.01 AS NET_FLOATING_VOLUME,
    ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS FLOATING_PROFIT,
        ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
	
	FROM live1.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
		(mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= (CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
    AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CMD <2 AND group_table.LIVE = 'live1' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    UNION
    (SELECT 'live2' AS LIVE,group_table.COUNTRY, 
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_FLOATING_VOLUME,
	SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_CLOSED_VOLUME,
		SUM(CASE WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 0) THEN mt4_trades.VOLUME WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 1) THEN -1*mt4_trades.VOLUME ELSE 0 END)*0.01 AS NET_FLOATING_VOLUME,
    ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live2.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS FLOATING_PROFIT,
	        ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
    FROM live2.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
		(mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= (CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
    AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CMD <2 AND group_table.LIVE = 'live2' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    UNION
    (SELECT 'live3' AS LIVE,group_table.COUNTRY, 
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_FLOATING_VOLUME,
	SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_CLOSED_VOLUME,
		SUM(CASE WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 0) THEN mt4_trades.VOLUME WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 1) THEN -1*mt4_trades.VOLUME ELSE 0 END)*0.01 AS NET_FLOATING_VOLUME,
    ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live3.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS FLOATING_PROFIT,
	        ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
    FROM live3.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
		(mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= (CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
    AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM live3.cambodia_exclude)


    AND mt4_trades.CMD <2 AND group_table.LIVE = 'live3' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    UNION
    (SELECT 'live5' AS LIVE,group_table.COUNTRY,  
    CASE WHEN LEFT(SYMBOL,1) = "." then SYMBOL ELSE LEFT(SYMBOL,6) END as SYMBOL1,
    SUM(CASE WHEN mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_FLOATING_VOLUME,
	SUM(CASE WHEN mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00' THEN mt4_trades.VOLUME ELSE 0 END)*0.01 AS SUM_CLOSED_VOLUME,
		SUM(CASE WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 0) THEN mt4_trades.VOLUME WHEN (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and mt4_trades.cmd = 1) THEN -1*mt4_trades.VOLUME ELSE 0 END)*0.01 AS NET_FLOATING_VOLUME,
    ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live5.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS FLOATING_PROFIT,
	        ROUND(SUM(CASE 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN mt4_trades.PROFIT+mt4_trades.SWAPS 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/7.78 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
    WHEN (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND mt4_trades.CLOSE_TIME != '1970-01-01 00:00:00') THEN (mt4_trades.PROFIT+mt4_trades.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) AS CLOSED_PROFIT
    FROM live5.mt4_trades,live5.group_table WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
		(mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= DATE_ADD(DATE_SUB((CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END),INTERVAL 1 DAY),INTERVAL 1 HOUR))
    AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CMD <2 AND group_table.LIVE = 'live5' AND LENGTH(mt4_trades.LOGIN)>4 GROUP BY group_table.COUNTRY, SYMBOL1)
    ) AS B 
    WHERE COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST')
    ORDER BY COUNTRY, SYMBOL""".format(ServerTimeDiff_Query=server_time_diff_str)

    # Want to get results for the above query, to get the Floating PnL
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result

    result_col = raw_result.keys() # The column names

    #end = datetime.datetime.now()
    #print("\nSaving [Pulling] floating PnL tool: {}s\n".format((end - start).total_seconds()))

    # Since the Country and Symbols are Primary keys,
    # We want to add up the sums.
    country_symbol = {}
    for r in result_data:
        country = r[1]
        # Want to get core value
        symbol = cfd_core_symbol(r[2])
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
                             "`datetime`) VALUES {}".format(" , ".join(result_array)))


    #print(insert_into_table)
    raw_result = db.engine.execute(insert_into_table)  # Want to insert into the table.




    # Insert into the Checking tools.
    if (update_tool_time == 1):
        Tool = "BGI_Float_History_Save"
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
    return render_template("Webworker_Country_Float.html", backgroud_Filename='css/World_Map.jpg', icon= "css/Globe.png", Table_name="Country Float 🌎", \
                           title=title, ajax_url=url_for('analysis.BGI_Country_Float_ajax', _external=True), ajax_clear_cookie_url=url_for("analysis.Clear_session_ajax", _external=True), header=header, setinterval=15,
                           description=description, replace_words=Markup(['(Client Side)']))



# Get live 1 time difference from server.
# SQL Table where aaron_misc_data` where item = 'live1_time_diff
def get_live1_time_difference():

    # MYSQL WEEKDAY FUNCTION
    #0 = Monday, 1 = Tuesday, 2 = Wednesday, 3 = Thursday, 4 = Friday, 5 = Saturday, 6 =Sunday

    server_time_diff_str = "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"
    sql_query = text(server_time_diff_str)

    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result

    return int(result_data[0][0])   # Return the integer value.


# Want to get the Live 1 START of the day timing
# Will do a comparison and get the time for start and end.
# Need to account for Live 5/MT5 timing.
# For today, if Live 1 hour < 23, will return 2 workind days ago 2300
def liveserver_Previousday_start_timing(live1_server_difference=6, hour_from_2300 = 0):

    # Weekday: Minus how many days.
    # 6 = Sunday, we want to take away 3 days, to Thursday starting.
    previous_day_start = {6:3, 0:4, 1:2, 2:2, 3:2, 4:2, 5:2}
    now = datetime.datetime.now()
    # + 1 hour to force it into the next day.
    live1_server_timing = now - datetime.timedelta(hours=live1_server_difference) + datetime.timedelta(hours=1)
    return_time = get_working_day_date(start_date=live1_server_timing, weekdays_count= -1 * previous_day_start[live1_server_timing.weekday()],
                                       weekdaylist=[0, 1, 2, 3,4,5,6])
    return_time = return_time.replace(hour=23, minute=0, second=0, microsecond=0)
    return_time = return_time + datetime.timedelta(hours=hour_from_2300)

    #print("server_Time:{}, start_time: {}".format(live1_server_timing, return_time))
    return return_time


# Using Live 5 timing as guide.
# Since live 5 uses 0000 - 2400
# Will minus 1 day, and take 2300 to give live 1 timing
def liveserver_Nextday_start_timing(live1_server_difference=6, hour_from_2300 = 0, time = 0):

    if time == 0:   # Check if we had a start time
        now = datetime.datetime.now()
    else:
        now = time
    # Weekday: Minus how many days.
    # 6 = Sunday. We want to go ahead how many days, if it' that weekday (in key)
    next_day_start = {0: 1, 1: 1, 2: 1, 3: 1, 4: 3, 5: 2, 6: 1}

    # + 1 hour to force it into the next day.
    live5_server_timing = now - datetime.timedelta(hours=live1_server_difference) + datetime.timedelta(hours=1)
    return_time = get_working_day_date(start_date=live5_server_timing,
                        weekdays_count=  next_day_start[live5_server_timing.weekday()],
                                       weekdaylist=[0, 1, 2, 3, 4, 5, 6])
    return_time = return_time - datetime.timedelta(days=1)  # minus 1 days, and use 2300hrs
    return_time = return_time.replace(hour=23, minute=0, second=0, microsecond=0)
    return_time = return_time + datetime.timedelta(hours=hour_from_2300)

    #print("server_Time:{}, start_time: {}".format(live1_server_timing, return_time))
    return return_time


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
def save_previous_day_PnL():

    # If PnL Has been saved already. We don't need to save it again.
    if check_previous_day_pnl_in_DB():
        #print("PnL for previous day has been saved.")
        return

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
    HAVING COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', "HK")
    ORDER BY COUNTRY, SYMBOL""".format(ServerTimeDiff_Query=ServerTimeDiff_Query, live123_Time_String=live123_Time_String,live5_Time_String=live5_Time_String)

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
                             "`revenue`, `update_time`) VALUES {} ON DUPLICATE KEY UPDATE VOLUME=VALUES(VOLUME), REVENUE=VALUES(REVENUE), UPDATE_TIME=UPDATE_TIME".format(" , ".join(result_array)))
    raw_result = db.engine.execute(insert_into_table)  # Want to insert into the table.

    return


# Query SQL to return the previous day's PnL By Country
def get_country_daily_pnl():

    # Want to check what is the date that we should be retrieving.
    # Trying to reduce the over-heads as much as possible.
    live1_server_difference = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()

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

    # If empty, we just want to return an empty data frame. So that the following merge will not cause any issues
    return_df = pd.DataFrame(result_data, columns=result_col) if len(result_data) > 0 else pd.DataFrame()
    return return_df


# Query SQL to return the previous day's PnL By Symbol
def get_symbol_daily_pnl():

    # Want to check what is the date that we should be retrieving.
    # Trying to reduce the over-heads as much as possible.
    live1_server_difference = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()

    live1_start_time = liveserver_Previousday_start_timing(live1_server_difference=live1_server_difference, hour_from_2300=5)
    date_of_pnl = "{}".format(live1_start_time.strftime("%Y-%m-%d"))

    sql_statement = """SELECT SYMBOL, SUM(VOLUME) AS VOLUME, SUM(REVENUE) AS REVENUE, DATE
            FROM aaron.`bgi_dailypnl_by_country_group`
            WHERE DATE = '{}' 
            AND COUNTRY in (SELECT DISTINCT(COUNTRY) from live5.group_table where BOOK = "B")
            AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST', 'HK')
            GROUP BY SYMBOL""".format(date_of_pnl)

    # Want to get results for the above query, to get the Floating PnL
    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)  # Select From DB
    result_data = raw_result.fetchall()     # Return Result
    result_col = raw_result.keys()  # The column names

    # If empty, we just want to return an empty data frame. So that the following merge will not cause any issues
    return_df = pd.DataFrame(result_data, columns=result_col) if len(result_data) > 0 else pd.DataFrame()
    return return_df


#
# # Want to delete from the table where min is not 1.
# # TODO: will need to improve this when MY gives the code
# def delete_from_floating_table():
#     #if get_machine_ip_address() == '192.168.64.73': # This keeps getting stuck. Only implement on 64.73 server.
#     print("Trying to delete from aaron.`BGI_Float_History_Save`")
#     #insert_into_table = text("DELETE FROM aaron.`BGI_Float_History_Save` WHERE MINUTE(datetime) <> 1")
#
#     async_sql_insert_raw(app=current_app._get_current_object(), sql_insert="DELETE FROM aaron.`BGI_Float_History_Save` WHERE MINUTE(datetime) <> 1")
#     # Will Asnc this.
#     # raw_result = db.engine.execute(insert_into_table)  # Want to insert into the table.

# Clear session data.
# Called when refreshing cookies.
@analysis.route('/Clear_session_ajax', methods=['GET', 'POST'])
@roles_required()
def Clear_session_ajax():
    #list_of_pop = ["live1_sgt_time_diff", "live1_sgt_time_update", "yesterday_pnl_by_country", "yesterday_pnl_by_symbol"]

    #delete_from_floating_table()

    list_of_pop = []
    # We want to clear everything, other then the system generated ones.
    for u in list(session.keys()):
        if u not in  ['_fresh', '_id', 'csrf_token']:
            session.pop(u, None)
            list_of_pop.append(u)
    return "Session Cleared: {}".format(", ".join(list_of_pop))



# Will alter the session details.
# does not return anything
def check_session_live1_timing():

    return_val = False  # If session timing is outdated, or needs to be updated.
    # Might need to set the session life time. I think?
    # Want to save some stuff in session so that we don't have to keep querying for it.
    if "live1_sgt_time_diff" in session and \
        "live1_sgt_time_update" in session and  \
        datetime.datetime.now() < session["live1_sgt_time_update"] :
        return_val = True
        #print(session.keys())
        #print("From session: {}. Next update time: {}".format(session['live1_sgt_time_diff'], session['live1_sgt_time_update']))
    else:
        print(session)
        Clear_session_ajax()    # Clear all cookies. And reload everything again.

        #print("Getting live1 time diff from SQL.")
        session['live1_sgt_time_diff'] = get_live1_time_difference()

        # Will get the timing that we need to update again. Want to get start of next working day in SGT
        # so, will need to ADD the hours back.
        session['live1_sgt_time_update'] = liveserver_Nextday_start_timing(
                    live1_server_difference=session['live1_sgt_time_diff'],
                        hour_from_2300=0) + datetime.timedelta(hours=session['live1_sgt_time_diff'], minutes=10)

    return return_val

# Get BGUI Float by country
@analysis.route('/BGI_Country_Float_ajax', methods=['GET', 'POST'])
def BGI_Country_Float_ajax():

    #start = datetime.datetime.now()
    # print(current_app.permanent_session_lifetime)
    # print(session)

    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}])


    #check_session_live1_timing()
    # Will check the timing
    if check_session_live1_timing() == True and "yesterday_pnl_by_country" in session:
        #print("From in memory")
        # From in memory of session
        #print("live1_sgt_time_update ' {}".format(session["live1_sgt_time_update"]))

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

    # sql_statement = """SELECT COUNTRY, SUM(ABS(VOLUME)) AS VOLUME, SUM(REVENUE) AS REVENUE, DATETIME
    #     FROM aaron.BGI_Float_History_Save,(
    #     SELECT DISTINCT(datetime) AS DT
    #     FROM aaron.BGI_Float_History_Save
    #     WHERE DATETIME >= (NOW() - INTERVAL 2 DAY)
    #     GROUP BY LEFT(DATETIME, 15)
    #     ) AS A
    #     WHERE BGI_Float_History_Save.datetime = A.DT
    #     GROUP BY COUNTRY, DATETIME
    #
    #     UNION
    #
    #     SELECT COUNTRY, SUM(ABS(VOLUME)) AS VOLUME, SUM(REVENUE) AS REVENUE, DATETIME
    #                 FROM aaron.BGI_Float_History_Save
    #                 WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.BGI_Float_History_Save)
    #                 GROUP BY COUNTRY"""

    sql_query = text(sql_statement)

    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result

    result_col = raw_result.keys()  # Column names



    #end = datetime.datetime.now()
    #print("\nGetting Country PnL tool[After Query]: {}s\n".format((end - start).total_seconds()))

    df = pd.DataFrame(result_data, columns=result_col)



    # We want to show Client Side for Dealing as well as A book Clients.
    df['FLOAT_REVENUE'] = df.apply(lambda x: -1*x['FLOAT_REVENUE'] if (x["COUNTRY"].find("_A") > 0 or x["COUNTRY"].find('Dealing') >= 0) else x['FLOAT_REVENUE'], axis=1)

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
    chart_Country = list(df_to_table['COUNTRY'].values)



    # Adding words to the country name, to be flagged out by Javascript to color the cell
    df_to_table['COUNTRY'] = df_to_table.apply(lambda x: '{} (Client Side)'.format(x['COUNTRY']) if (
                x["COUNTRY"].find("_A") > 0 or x["COUNTRY"].find('Dealing') >= 0) else x['COUNTRY'], axis=1)

    # Don't want the zeros. 0 is discarded.
    datetime_pull =  [c for c in list(df_to_table['DATETIME'].unique()) if c != 0] if  "DATETIME" in df_to_table else  ["No Datetime in df."]

    # if 'YESTERDAY_DATE' in df_to_table:  # Want to get the date as date, without the time!
    #     print(df_to_table['YESTERDAY_DATE'].apply(type))
    #     df_to_table['YESTERDAY_DATE'] = df_to_table['YESTERDAY_DATE'].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime.date) else x)
    #     print( df_to_table['YESTERDAY_DATE'])
    #     print(df_to_table['YESTERDAY_DATE'].apply(type))

    yesterday_datetime_pull = [c for c in list(df_to_table['YESTERDAY_DATE'].unique()) if c != 0] if "YESTERDAY_DATE" in df_to_table else ["No YESTERDAY_DATE in df."]
    #print(datetime_pull)
    #print(yesterday_datetime_pull)

    # If we need to rename the columns. We need to change here as well! WE removed "DATETIME", "YESTERDAY_DATE",
    show_col = ["COUNTRY", "FLOAT_VOLUME", "FLOAT_REVENUE" ,"CLOSED_VOL", "CLOSED_REVENUE", "YESTERDAY_REVENUE", "YESTERDAY_VOLUME"]
    df_show_col = [d for d in show_col if d in df_to_table]
    #print(df_show_col)


    # Reduce the number of columns.
    df_to_table = df_to_table[df_show_col]

    df_records = df_to_table.to_records(index=False)
    dataframe_col = list(df_to_table.columns)
    df_records = [list(a) for a in df_records]

    #print(emoji.emojize('Python is :china: :TW: :NZ: :HK:'))

    # Want to clean up the data
    result_clean = [[Get_time_String(d) if isinstance(d, datetime.datetime) else d for d in r] for r in result_data]

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

    title = "Symbol Float"
    header = "Symbol Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup("<b>Floating PnL By Symbol.</b><br> Revenue = Profit + Swaps<br>"+
                         "Includes B book Groups. Includes HK as well.<br>"+
                         'Using Live5.group_table where book = "B"<br>' +
                          'HK Is excluded from all symbols <br>'  +
                         'Values are all on <b>BGI Side</b>. <br>' +
                         'Sort by absolute net volume.<br>'+
                         "Yesterday Data saved in cookies.<br>" +
                         "Taking Live prices from Live 1 q Symbols")


        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename='css/pattern7.jpg', icon= "", Table_name="Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('analysis.BGI_Symbol_Float_ajax', _external=True), header=header, setinterval=15,
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

    if check_session_live1_timing() == True and "yesterday_pnl_by_symbol" in session:
        # From "in memory" of session

        df_yesterday_symbol_pnl = pd.DataFrame.from_dict(session["yesterday_pnl_by_symbol"])
    else:       # If session timing is outdated, or needs to be updated.

        print("Getting yesterday symbol PnL from DB")
        df_yesterday_symbol_pnl = get_symbol_daily_pnl()
        if "DATE" in df_yesterday_symbol_pnl:  # We want to save it as a string.
            #print("DATE IN")
            df_yesterday_symbol_pnl['DATE'] = df_yesterday_symbol_pnl['DATE'].apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime.date) else x)
        session["yesterday_pnl_by_symbol"] =  df_yesterday_symbol_pnl.to_dict()

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

    #server_time_diff_str = "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"

    # sql_statement = """SELECT SYMBOL, SUM(ABS(floating_volume)) AS VOLUME, -1 * SUM(net_floating_volume) AS NETVOL,
    #                         SUM(floating_revenue) AS REVENUE,
    #                         SUM(closed_vol_today) as "TODAY_VOL",
    #                         SUM(closed_revenue_today) as "TODAY_REVENUE",
    #                         DATE_ADD(DATETIME,INTERVAL ({ServerTimeDiff_Query}) HOUR) AS DATETIME
    #         FROM aaron.BGI_Float_History_Save
    #         WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.BGI_Float_History_Save)
    #         AND COUNTRY IN (SELECT COUNTRY from live5.group_table where BOOK = "B")
    #         GROUP BY SYMBOL
    #         ORDER BY REVENUE DESC
    #         """.format(ServerTimeDiff_Query=server_time_diff_str)

    sql_statement = """SELECT aaron.BGI_Float_History_Save.SYMBOL, SUM(ABS(floating_volume)) AS VOLUME, -1 * SUM(net_floating_volume) AS NETVOL, 
                    SUM(floating_revenue) AS REVENUE, 
                    SUM(closed_vol_today) as "TODAY_VOL",
                    SUM(closed_revenue_today) as "TODAY_REVENUE",                            
                                                P.ASK, P.BID,
                                                DATE_ADD(DATETIME,INTERVAL ({ServerTimeDiff_Query}) HOUR) AS DATETIME
    FROM aaron.BGI_Float_History_Save 
                    LEFT JOIN live1.mt4_prices as P ON  CONCAT(aaron.BGI_Float_History_Save.SYMBOL, "q") = P.SYMBOL
    WHERE DATETIME = (SELECT MAX(DATETIME) FROM aaron.BGI_Float_History_Save)
    AND COUNTRY IN (SELECT COUNTRY from live5.group_table where BOOK = "B") and COUNTRY not like "HK"
    GROUP BY SYMBOL
    ORDER BY REVENUE DESC""".format(ServerTimeDiff_Query=server_time_diff_str)


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


    # yesterday_datetime_pull = [c for c in list(df_yesterday_symbol_pnl['DATE'].unique()) if
    #                            c != 0] if "DATE" in df_yesterday_symbol_pnl else ["No YESTERDAY_DATE in df."]
    #


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

    # Go ahead to merge the tables.
    if "SYMBOL" in df_to_table and "SYMBOL" in df_yesterday_symbol_pnl:
        df_to_table = df_to_table.merge(df_yesterday_symbol_pnl, on="SYMBOL", how='left')
        df_to_table.fillna("-", inplace=True)  # Want to fill up all the empty ones with -




    # Need to check if the columns are in the df.
    # taking this chance to re-arrange them as well.
    col_of_df = [c for c in ["SYMBOL", "NETVOL", "VOLUME", "REVENUE", "TODAY_VOL", "TODAY_REVENUE", "BID", "ASK","YESTERDAY_VOLUME", "YESTERDAY_REVENUE"] if c in  list(df_to_table.columns)]

    df_records = df_to_table[col_of_df].to_records(index=False)
    df_records = [list(a) for a in df_records]

    return_val = [dict(zip(col_of_df,d)) for d in df_records]

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
@analysis.route('/Open_Symbol/<symbol>', methods=['GET', 'POST'])
@roles_required()
def symbol_float_trades_ajax(symbol=""):
    symbol="XAUUSD"
    symbol_condition = "AND SYMBOL Like '%{}%'".format(symbol)
    sql_statement = """(SELECT 'live1' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
        FROM live1.mt4_trades, live5.group_table 
        WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= (CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live1' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition}
            AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST'))
    UNION 
        (SELECT 'live2' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM live2.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= (CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live2' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition}
            AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST'))
    UNION 
        (SELECT 'live3' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM live3.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= (CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live3' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition}
            AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST'))
    UNION
        (SELECT 'live5' AS LIVE,group_table.COUNTRY, LOGIN, TICKET,
        SYMBOL, CMD,
        VOLUME*0.01 as LOTS, OPEN_PRICE,
            OPEN_TIME, CLOSE_PRICE, CLOSE_TIME, SWAPS, PROFIT, mt4_trades.`GROUP`
            FROM live5.mt4_trades,live5.group_table 
            WHERE mt4_trades.`GROUP` = group_table.`GROUP` AND 
            (mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' or mt4_trades.CLOSE_TIME >= DATE_ADD(DATE_SUB((CASE WHEN HOUR(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR)) < 23 THEN DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),INTERVAL 1 DAY),'%Y-%m-%d 23:00:00') ELSE DATE_FORMAT(DATE_SUB(NOW(),INTERVAL ({ServerTimeDiff_Query}) HOUR),'%Y-%m-%d 23:00:00') END),INTERVAL 1 DAY),INTERVAL 1 HOUR))
        AND LENGTH(mt4_trades.SYMBOL)>0 
            AND mt4_trades.CMD <2 
            AND group_table.LIVE = 'live5' 
            AND LENGTH(mt4_trades.LOGIN)>4
            {symbol_condition}
            AND COUNTRY NOT IN ('Omnibus_sub','MAM','','TEST'))""".format(symbol_condition=symbol_condition, ServerTimeDiff_Query=6)

    sql_query = text(sql_statement)
    raw_result = db.engine.execute(sql_query)   # Insert select..
    result_data = raw_result.fetchall()     # Return Result




@analysis.route('/analysis/cn_live_vol_ajax')
@roles_required()
# Gets the cn df, and uses it to plot the various charts.
def cn_live_vol_ajax():

    # start = datetime.datetime.now()
    # df = get_cn_df()
    # bar = plot_open_position_net(df, chart_title = "[CN] Open Position")
    # cn_pnl_bar = plot_open_position_revenue(df, chart_title="[CN] Open Position Revenue")
    # cn_heat_map = plot_volVSgroup_heat_map(df,chart_title="[CN] Net Position by Group")
    # print("Getting cn df and charts {} Seconds.".format((datetime.datetime.now()-start).total_seconds()))
    # vol_sum = round(sum(df['VOLUME']),2)
    # net_vol_sum = round(sum(df['NET_VOLUME']),2)
    # revenue_sum = round(sum(df['REVENUE']),2)
    # cn_summary = {'COUNTRY' : 'CN', 'VOLUME': vol_sum, "NET VOLUME": net_vol_sum, "REVENUE" : revenue_sum, 'TIME': Get_time_String()}
    # print(cn_summary)
    # return json.dumps([bar, cn_pnl_bar, cn_heat_map, cn_summary], cls=plotly.utils.PlotlyJSONEncoder)

    return get_country_charts(country="CN", df= get_cn_df())



@analysis.route('/analysis/tw_live_vol_ajax')
@roles_required()
# Gets the cn df, and uses it to plot the various charts.
def tw_live_vol_ajax():
    return get_country_charts(country="TW", df= get_tw_df())

# Generic get the country charts.
def get_country_charts(country, df):

    start = datetime.datetime.now()
    bar = plot_open_position_net(df, chart_title = "[{}] Open Position".format(country))
    pnl_bar = plot_open_position_revenue(df, chart_title="[{}] Open Position Revenue".format(country))
    heat_map = plot_volVSgroup_heat_map(df,chart_title="[{}] Net Position by Group".format(country))
    #print("Getting {} df and charts {} Seconds.".format(country,(datetime.datetime.now()-start).total_seconds()))
    vol_sum = '{:,.2f}'.format(round(sum(df['VOLUME']),2))
    revenue_sum = '{:,.2f}'.format(round(sum(df['REVENUE']),2))
    summary = {'COUNTRY' : country, 'VOLUME': vol_sum, 'REVENUE' : revenue_sum, 'TIME': Get_time_String()}
    #print(cn_summary)
    return json.dumps([bar, pnl_bar, heat_map, summary], cls=plotly.utils.PlotlyJSONEncoder)



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

    # Want to get the duration string
    df_data["DURATION_STR"] = df_data.apply(lambda x: trade_duration_bin((x["CLOSE_TIME"] - x["OPEN_TIME"]).seconds), axis=1)
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
                           title=title, header=header,
                           form=form, description=description)


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

    # Table names will need be in a dict, identifying if the table should be horizontal or vertical.
    # Will try to do smaller vertical table to put 2 or 3 tables in a row.
    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename='css/pattern5.jpg', icon="",
                           Table_name={"Live: {}, Login: {}".format(Live, Login):"V",
                                       "Profit Calculation":"V",
                                       "Net Position": "H",
                                       "Open Trades": "H",
                                       "Past Trades" : "H",
                                       "Deposit/Withdrawal plot":"P",
                                       "Average Trade Timings":"P"},
                           title=title,
                           ajax_url=url_for('analysis.Client_trades_Analysis_ajax',_external=True, Live=Live, Login=Login),
                           header=header,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"]))

# To remove the account from being excluded.
@analysis.route('/Client_Trades_ajax/<Live>/<Login>', methods=['GET', 'POST'])
@roles_required()
def Client_trades_Analysis_ajax(Live="", Login=""):


    if Live not in ["1","2","3","5"] or Login == "":   # There are no information.
        return json.dumps([{"Result":"Error in Login or Live"}])

    # sql_statement = """SELECT LOGIN, ENABLE, ENABLE_READONLY, BALANCE, CREDIT, EQUITY, `GROUP`
    #         FROM live{Live}.mt4_users
    #         WHERE `Login`='{Login}'""".format(Live=Live, Login=Login)
    #
    # result = Query_SQL_db_engine(sql_statement)


    sql_statement = """SELECT LOGIN, `GROUP`, `ENABLE`, ENABLE_READONLY, `NAME`, 
                        ROUND(BALANCE,2) as BALANCE, ROUND(CREDIT , 2) as CREDIT,
                        ROUND(EQUITY, 2) as EQUITY, ROUND(MARGIN, 2) as `MARGIN (E/M)`, 
                        ROUND(MARGIN_LEVEL,2) as MARGIN_LEVEL, ROUND(MARGIN_FREE, 2) as MARGIN_FREE
            FROM live{Live}.mt4_users 
            WHERE `Login`='{Login}'""".format(Live=Live, Login=Login)

    sql_statement = sql_statement.replace("\n", "").replace("\t", "")
    login_details = Query_SQL_db_engine(sql_statement)

    if len(login_details) <= 0:   # There are no information.
        return json.dumps([{"Result":"Error in Login or Live"}])


    # # Write the SQL Statement and Update to disable the Account monitoring.
    # # Want the CLOSE TRADES limited to 100
    # # AND all the OPEN trades
    sql_statement = """(SELECT TICKET, SYMBOL, VOLUME * 0.01 AS LOTS, CMD, OPEN_TIME, 
		CLOSE_TIME, SWAPS, PROFIT, `COMMENT`, `GROUP`
        FROM live{Live}.mt4_trades 
        WHERE `Login`='{Login}' AND CLOSE_TIME = "1970-01-01 00:00:00" AND CMD < 2)
        
        UNION
        
        (SELECT TICKET, SYMBOL, VOLUME * 0.01 AS LOTS, CMD, OPEN_TIME, 
                CLOSE_TIME, SWAPS, PROFIT, `COMMENT`, `GROUP`
        FROM live{Live}.mt4_trades 
        WHERE `Login`='{Login}' and CLOSE_TIME <> "1970-01-01 00:00:00"  AND CMD < 2
        ORDER BY CLOSE_TIME DESC
        LIMIT 100 )""".format(Live=Live, Login=Login)

    sql_statement = sql_statement.replace("\n", "").replace("\t", "")
    result = Query_SQL_db_engine(sql_statement)
    df_data = pd.DataFrame(result)

    #
    # # symbol_average_tradetime = Average_trade_time_per_symbol(df_data)
    #
    # # average_trade_duration_fig = plot_symbol_tradetime_duration(symbol_average_tradetime)


    average_trade_duration_fig = plot_symbol_tradetime_duration_2(df_data)

    # # Can use Pandas to calculate the average as well...
    # """select SYMBOL, AVG(CLOSE_TIME-OPEN_TIME) as 'AVERAGE DURATION'
    # FROM live2.mt4_trades
    # WHERE login = 2040 and CMD <2 and CLOSE_TIME != "1970-01-01 00:00:00"
    # AND CLOSE_TIME > DATE_SUB(NOW(),INTERVAL 1 MONTH)
    # GROUP BY SYMBOL
    # ORDER BY 'AVERAGE DURATION' DESC"""


    # sql_statement = """SELECT LOGIN, `GROUP`, `ENABLE`, ENABLE_READONLY, `NAME`, BALANCE, CREDIT, EQUITY, MARGIN, MARGIN_LEVEL, MARGIN_FREE
    #         FROM live{Live}.mt4_users
    #         WHERE `Login`='{Login}'""".format(Live=Live, Login=Login)
    #
    # sql_statement = sql_statement.replace("\n", "").replace("\t", "")
    # login_details = Query_SQL_db_engine(sql_statement)
    #df_data = pd.DataFrame(result)

    # Get net positions for all.
    net_position = Calculate_Net_position(df_data)
    net_position_dict = net_position.to_dict("record")
    net_position_dict_clean = [{k: "{}".format(d) for k, d in r.items()} for r in net_position_dict]

    # Want to get total deposit, withdrawal.. etc.
    Sum_details = Sum_total_account_details(Live, Login)
    deposit_withdrawal_fig = plot_account_details(Sum_details)   # To get the figure to show.

    if "PROFIT" in df_data:     # BOLD the profit
        df_data["PROFIT"] =  df_data["PROFIT"].apply(lambda x: '<span style="color:{Color}">{value}</span>'.format(Color=color_negative_red(x), value=x))

    if "SWAPS" in df_data:
        df_data["SWAPS"] =  df_data["SWAPS"].apply(lambda x: '<span style="color:{Color}">{value}</span>'.format(Color=color_negative_red(x), value=x))

    # Want to calculate the time duration. Let format deal with printing it.
    # If the trade is closed, we want to find out how long it was opened for.
    # If it's still opened, we want to know how long..
    # We don't care about the BALANCE, CREDIT, and the sell/buy stop/limit

    df_data["DURATION"] = df_data.apply(lambda x: "-" if int(x['CMD']) >= 2 else \
                        (x["CLOSE_TIME"] - x["OPEN_TIME"] \
                        if x["CLOSE_TIME"] != pd.Timestamp('1970-01-01 00:00:00') else \
                        pd.Timestamp.now() - x["OPEN_TIME"]), axis=1)

    # Want to see which trades are below 3 mins..
    df_data["DURATION"] =  df_data["DURATION"].apply(lambda x: '<span style="color:red">{value}</span>'.format(value=x)  \
                            if isinstance(x, datetime.timedelta) and x < datetime.timedelta(seconds=180) else x )



    cmd = {0: "BUY", 1: "SELL", 2: "BUY LIMIT", 3:"SELL LIMIT", 4: "BUY STOP", 5: "SELL STOP", 6: "BALANCE", 7: "CREDIT"}

    if "CMD" in df_data:
        df_data["CMD"] = df_data["CMD"].apply(lambda x: cmd[x] if x in cmd else x)

    # Re-arrange the index
    df_data = df_data[[ 'TICKET', 'SYMBOL', 'CMD','LOTS', 'OPEN_TIME',
                        'CLOSE_TIME',"DURATION",'SWAPS', 'PROFIT','GROUP',
                        'COMMENT', 'DURATION_STR']]

    # Want to get the open trades.
    open_position =  df_data[df_data["CLOSE_TIME"] == pd.Timestamp('1970-01-01 00:00:00')] # Only open trades.
    open_position.drop(columns=['CLOSE_TIME', 'DURATION'], inplace=True)
    open_position_dict = open_position.to_dict("record")

    # Want to get the duration string
    df_data["DURATION_STR"] = df_data.apply(lambda x: (x["OPEN_TIME"] - x["CLOSE_TIME"]).seconds, axis=1)

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

    # Return "Trades" and "Net position"
    return json.dumps({"V1" : login_details, "V2": Sum_details, "H2": net_position_dict_clean,
                       "H3" : open_position_dict,
                       "H1": closed_trades,
                        "P1":deposit_withdrawal_fig,
                       "P2": average_trade_duration_fig}, cls=plotly.utils.PlotlyJSONEncoder)


def color_negative_red(value):
      # """
      # Colors elements in a dateframe
      # green if positive and red if
      # negative. Does not color NaN
      # values.
      # """

    if value < 0:
        color = 'red'
    elif value > 0:
        color = 'green'
    else:
        color = 'black'
    #return 'color: %s' % color
    return  color

