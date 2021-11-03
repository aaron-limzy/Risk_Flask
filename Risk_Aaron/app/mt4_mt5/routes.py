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

mt4_mt5_bp = Blueprint('mt4_mt5', __name__)





@mt4_mt5_bp.route('/BGI_MT4_Symbol_Float2', methods=['GET', 'POST'])
@roles_required()
def BGI_MT4_Symbol_Float2():

    title = "Symbol Float"
    header = "Symbol Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup("<b>Floating PnL By Symbol.</b><br> Revenue = Profit + Swaps<br>"+
                         "Includes B book Groups. Includes HK as well.<br>"+
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
                           Table_name="Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('mt4_mt5.BGI_MT4_Symbol_Float_ajax2', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(Client Side)']))


# Get BGI Float by Symbol
@mt4_mt5_bp.route('/BGI_MT4_Symbol_Float_ajax2', methods=['GET', 'POST'])
@roles_required()
def BGI_MT4_Symbol_Float_ajax2():


    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    # Get all the data that is pertainint to MT4 Floating.
    df_to_table = mt4_Symbol_Float_Data()

    # print(df_to_table)
    # print()
    yesterday_datetime_pull = ["No YESTERDAY_DATE in df."]
    # We already know the date. No need to carry on with this data.
    if "YESTERDAY_DATE" in df_to_table:

        # Want to get the Unique date for the "Yesterday" date.
        # Want to know the date of the symbol "yesterday" details.
        yesterday_datetime_pull = [c for c in list(df_to_table[df_to_table['YESTERDAY_DATE'].notna()]['YESTERDAY_DATE'].unique()) if c != 0]

        # We have no need for the column anymore.
        df_to_table.pop('YESTERDAY_DATE')


    #end = datetime.datetime.now()
    #print("\nGetting SYMBOL PnL tool[After Query]: {}s\n".format((end - start).total_seconds()))

    datetime_pull = ["No Datetime in df."]   # Default Value
    if "DATETIME" in df_to_table:
        # Get Datetime into string
        df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x))
        datetime_pull =  [c for c in list(df_to_table['DATETIME'].unique()) if c != 0]
        df_to_table.pop('DATETIME')


    # Sort by abs net volume
    df_to_table["ABS_NET"] = df_to_table["NETVOL"].apply(lambda x: abs(x))
    df_to_table.sort_values(by=["ABS_NET"], inplace=True, ascending=False)
    df_to_table.pop('ABS_NET')


    # SQL sometimes return values with lots of decimal points.
    # We only want to show afew. Else, it takes up too much screen space.
    # Want to do this for both BID and ASK column
    col_from_exp_to_str = ["BID", "ASK"]
    for c in col_from_exp_to_str:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: "{:2.5f}".format(x) if (isfloat(decimal.Decimal(str(x)).as_tuple().exponent)
                                                                                            and (decimal.Decimal(str(x)).as_tuple().exponent < -5)) else x)

    # Time to fill in the NAs
    df_to_table.fillna("-", inplace=True)

    # Hyperlink this.
    if "YESTERDAY_REVENUE" in df_to_table:
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


    # Want to hyperlink it.
    df_to_table["SYMBOL"] = df_to_table["SYMBOL"].apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{symbol}</a>'.format(symbol=x,
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x, book="b")))

    if "TODAY_VOL" in df_to_table:
    # Want to hyperlink it.
        df_to_table["TODAY_VOL"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{TODAY_VOL}</a>'.format(TODAY_VOL=x['TODAY_VOL'],
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x['SYMBOL'], book="b")), axis=1)
    if "VOLUME" in df_to_table:
        # Want to hyperlink it.
        df_to_table["VOLUME"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{VOLUME}</a>'.format(VOLUME=x['VOLUME'],
                                                                        url=url_for('analysis.symbol_float_trades', _external=True, symbol=x['SYMBOL'], book="b")), axis=1)

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

def mt4_Symbol_Float_Data():

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

    # Want to change the names of the columns. To add in the word "yesterday".
    # But we need to preserve the SYMBOL name since we use that to merge.
    df_yesterday_symbol_pnl.rename(columns=dict(zip(["{}".format(c) for c in list(df_yesterday_symbol_pnl.columns) if c.find("SYMBOL") == -1], \
        ["YESTERDAY_{}".format(c) for c in list(df_yesterday_symbol_pnl.columns) if c.find("SYMBOL") == -1])), inplace=True)

    # In the case that it's an empty dataframe, we need to artificially create the column, else, it can't be merged.
    if "SYMBOL" not in df_yesterday_symbol_pnl:
        df_yesterday_symbol_pnl["SYMBOL"] = ""


    # ------ Now to get the Floating Data. -----
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


    result = query_SQL_return_record(text(sql_statement))
    df_floating = pd.DataFrame(result)

    # In the case that it's an empty dataframe, we need to artificially create the column, else, it can't be merged.
    if "SYMBOL" not in df_floating:
        df_floating["SYMBOL"] = ""

    # For the display of table, we only want the latest data input.
    if 'DATETIME' in df_floating:
        df_floating = df_floating[df_floating['DATETIME'] == df_floating['DATETIME'].max()].drop_duplicates()

    # Go ahead to merge the tables, and add the hyperlink
    if "SYMBOL" in df_floating and "SYMBOL" in df_yesterday_symbol_pnl:
        df_to_table = df_floating.merge(df_yesterday_symbol_pnl, on="SYMBOL", how='outer')
        #df_to_table.fillna("-", inplace=True)  # Want to fill up all the empty ones with -

    return df_to_table


# Want to get the moving average of all the symbols from MT4
def mt4_symbol_average_data():

    # sql_statement = """ SELECT DATE, Basesymbol, MA10_VOL as `CLOSE_MA10`
    #      FROM aaron.sf_daily_close_vol as sf
    #      WHERE sf.DATE in (SELECT CASE WEEKDAY(NOW())
    #                                                     WHEN 6 THEN DATE(DATE_SUB(NOW(), INTERVAL 2 DAY))
    #                                                     WHEN 0 THEN DATE(DATE_SUB(NOW(), INTERVAL 3 DAY))
    #                                                     ELSE DATE(DATE_SUB(NOW(), INTERVAL 1 DAY))
    #                                             END as `Last_Weekday`
    #                                             )
    #     """
    sql_statement = """ call aaron.`sf_BBOOK_Daily_close_MA10`"""

    result = query_SQL_return_record(text(sql_statement))
    result = pd.DataFrame(result)
    return result


@mt4_mt5_bp.route('/BGI_All_Symbol_Float', methods=['GET', 'POST'])
@roles_required()
def BGI_All_Symbol_Float():

    title = "MT4/MT5 Symbol Float"
    header = "MT4/MT5 Symbol Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup("<b>Floating PnL By Symbol for MT4 and MT5.</b><br>"+
                         "Includes B book Groups. <br>"+
                         'Using Live5.group_table where book = "B" for MT4 Groups<br>' +
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
                         "Taking Live prices from Live 1 q Symbols.<br>" + \
                         "For ticks that Live 1 dosn't have, Ticks are taken from Live 2.")




        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename=background_pic("BGI_Symbol_Float"),
                           icon= "",
                           Table_name="MT4/MT5 Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('mt4_mt5.BGI_All_Symbol_Float_ajax', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(High)']))


# Get BGI Float by Symbol
@mt4_mt5_bp.route('/BGI_All_Symbol_Float_ajax', methods=['GET', 'POST'])
@roles_required()
def BGI_All_Symbol_Float_ajax():



    # start_time = datetime.datetime.now()
    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    # Get all the data that is pertainint to MT4 Floating.
    df_mt4 = mt4_Symbol_Float_Data()
    # print("Time taken for MT4 call: {}".format((datetime.datetime.now() - start_time).total_seconds()))
    start_time = datetime.datetime.now()
    df_mt5 = mt5_symbol_float_data()

    # df_mt4_average = mt4_symbol_average_data()
    #print(df_mt4_average)

    # print("Time taken for MT5 Call: {}".format((datetime.datetime.now() - start_time).total_seconds()))

    # print()
    # print(df_mt4.columns)
    # print()
    # print(df_mt5.columns)

    #MT4 #['SYMBOL', 'VOLUME', 'NETVOL', 'REVENUE', 'TODAY_VOL', 'TODAY_REVENUE', 'ASK', 'BID', 'DATETIME', 'YESTERDAY_VOLUME', 'YESTERDAY_REVENUE', 'YESTERDAY_DATE'],
    #MT5 %['SYMBOL', 'FLOATING_LOTS', 'NET_LOTS', 'REVENUE', 'TODAY_LOTS', 'TODAY_REVENUE', 'YESTERDAY_LOTS', 'YESTERDAY_REVENUE', 'YESTERDAY_REBATE', 'YESTERDAY_DATETIME_PULL', 'DATETIME']

    df_mt4.rename(columns={"NETVOL": "NET_LOTS", "VOLUME": "FLOATING_LOTS", "TODAY_VOL" : "TODAY_LOTS", "YESTERDAY_VOLUME": "YESTERDAY_LOTS"}, inplace=True)

    # Clearing up columns that is not needed.
    if "DATETIME" in df_mt5:
        df_mt5.pop("DATETIME")
    if "YESTERDAY_DATETIME_PULL" in df_mt5:
        df_mt5.pop("YESTERDAY_DATETIME_PULL")
    if "YESTERDAY_REBATE" in df_mt5:
        df_mt5.pop("YESTERDAY_REBATE")


    # Concat the MT4 and MT5 Tables
    df_to_table = pd.concat([df_mt4, df_mt5], ignore_index=True)

    # Want to get the ticks that Live 1 don't have from Live 2.
    # Need to do some Clean up for CFDs as well.
    missing_ticks_df = get_live2_ask_bid(list(df_to_table[df_to_table['BID'].isnull()]['SYMBOL'].unique()) + [".DE30"])


    if 'MODIFY_TIME' in missing_ticks_df:
        missing_ticks_df.pop("MODIFY_TIME")
    df_to_table = pd.concat([df_to_table, missing_ticks_df], ignore_index=True)


    # Want to Aggregate the columns accordingly.
    df_to_table = df_to_table.groupby("SYMBOL").agg(
        NET_LOTS=           pd.NamedAgg(column="NET_LOTS", aggfunc="sum"),
        FLOATING_LOTS=      pd.NamedAgg(column="FLOATING_LOTS", aggfunc="sum"),
        REVENUE=            pd.NamedAgg(column="REVENUE", aggfunc="sum"),
        TODAY_LOTS=         pd.NamedAgg(column="TODAY_LOTS", aggfunc="sum"),
        TODAY_REVENUE=      pd.NamedAgg(column="TODAY_REVENUE", aggfunc="sum"),
        BID=                pd.NamedAgg(column="BID", aggfunc="mean"),
        ASK=                pd.NamedAgg(column="ASK", aggfunc="mean"),
        YESTERDAY_LOTS=     pd.NamedAgg(column="YESTERDAY_LOTS", aggfunc="sum"),
        YESTERDAY_REVENUE=  pd.NamedAgg(column="YESTERDAY_REVENUE", aggfunc="sum")   ).reset_index()

    # # pd.set_option('display.max_columns', None)
    # print()
    # # df_to_table = pd.concat([df_mt4, df_mt5], ignore_index=True)
    # print(df_to_table)

    # Want to merge it to compare it with the average of MT4 to compare.
    # df_to_table = df_to_table.merge(df_mt4_average, left_on="SYMBOL", right_on="Basesymbol", how="left")
    #
    #
    # print(df_to_table)

    #['SYMBOL', 'FLOATING_LOTS', 'NET_LOTS', 'REVENUE', 'TODAY_LOTS','TODAY_REVENUE', 'ASK', 'BID', 'DATETIME', 'YESTERDAY_LOTS','YESTERDAY_REVENUE', 'YESTERDAY_DATE']

    # print(df_to_table)
    # print()
    yesterday_datetime_pull = ["No YESTERDAY_DATE in df."]
    # We already know the date. No need to carry on with this data.
    if "YESTERDAY_DATE" in df_to_table:

        # Want to get the Unique date for the "Yesterday" date.
        # Want to know the date of the symbol "yesterday" details.
        yesterday_datetime_pull = [c for c in list(df_to_table[df_to_table['YESTERDAY_DATE'].notna()]['YESTERDAY_DATE'].unique()) if c != 0]

        # We have no need for the column anymore.
        df_to_table.pop('YESTERDAY_DATE')


    #end = datetime.datetime.now()
    #print("\nGetting SYMBOL PnL tool[After Query]: {}s\n".format((end - start).total_seconds()))

    datetime_pull = ["No Datetime in df."]   # Default Value
    if "DATETIME" in df_to_table:
        # Get Datetime into string
        df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x) if isinstance(x, pd.Timestamp) else x)
        #print(df_to_table['DATETIME'])
        datetime_pull =  [c for c in list(df_to_table['DATETIME'][df_to_table['DATETIME'].notnull()].unique()) if c != 0]
        df_to_table.pop('DATETIME')



    # Sort by abs net volume
    df_to_table["ABS_NET"] = df_to_table["NET_LOTS"].apply(lambda x: abs(x))
    df_to_table.sort_values(by=["ABS_NET"], inplace=True, ascending=False)
    df_to_table.pop('ABS_NET')


    # SQL sometimes return values with lots of decimal points.
    # We only want to show afew. Else, it takes up too much screen space.
    # Want to do this for both BID and ASK column
    col_from_exp_to_str = ["BID", "ASK"]
    for c in col_from_exp_to_str:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: "{:2.5f}".format(float(x)) if (isfloat(decimal.Decimal(str(x)).as_tuple().exponent)
                                                                                            and (decimal.Decimal(str(x)).as_tuple().exponent < -5)) else x)

    # Time to fill in the NAs
    df_to_table.fillna("-", inplace=True)



    # Hyperlink this.
    if "YESTERDAY_REVENUE" in df_to_table:
        # Hyperlink it.
        df_to_table["YESTERDAY_REVENUE"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_REVENUE}</a>""".format( \
                                                            url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                            YESTERDAY_REVENUE=profit_red_green(x["YESTERDAY_REVENUE"]) if isfloat(x["YESTERDAY_REVENUE"]) else x["YESTERDAY_REVENUE"] ),
                                                            axis=1)


    # Also want to hyperlink this.
    # Just.. to have more hypterlink. HA ha ha.
    # Haven changed name yet. So it's still names "volume"
    if "YESTERDAY_LOTS" in df_to_table:
        # Hyperlink it.
        df_to_table["YESTERDAY_LOTS"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_LOTS}</a>""".format( \

                                                            url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                            YESTERDAY_LOTS=round(x["YESTERDAY_LOTS"], 2) if isfloat(x["YESTERDAY_LOTS"]) else x["YESTERDAY_LOTS"]),
                                                            axis=1)


    if "TODAY_LOTS" in df_to_table:
        # Want to hyperlink it.
        # Will need to write in '(High)' so that Javascript can pick it up and highlight the cell
        df_to_table["TODAY_LOTS"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{TODAY_LOTS:.2f}</a>'.format( \
                                                        TODAY_LOTS=x['TODAY_LOTS'],
                                                        url=url_for('analysis.symbol_float_trades',
                                                                    _external=True, symbol=x['SYMBOL'],
                                                                    book="b")),
                                                      axis=1)


    if "FLOATING_LOTS" in df_to_table:
        # Want to hyperlink it.
        df_to_table["FLOATING_LOTS"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{FLOATING_LOTS'
                                                                   '}</a>'.format(\
                                                                        FLOATING_LOTS="{:.2f}".format(x['FLOATING_LOTS']) if isfloat(x['FLOATING_LOTS']) else x['FLOATING_LOTS'],
                                                                        url=url_for('analysis.symbol_float_trades', _external=True, symbol=x['SYMBOL'], book="b")), axis=1)


    # Want to hyperlink it.
    df_to_table["SYMBOL"] = df_to_table["SYMBOL"].apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{symbol}</a>'.format(symbol=x,
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x, book="b")))


   # # # Want to add colors to the words.
    # Want to color the REVENUE
    cols = ["REVENUE", "TODAY_REVENUE", "NET_LOTS"]
    for c in cols:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: """{c}""".format(c=profit_red_green(x) if isfloat(x) else x))

    #Rename the VOLUME to LOTs
    # df_to_table.rename(columns={"NETVOL": "NET_LOTS", "VOLUME": "FLOATING_LOTS",
    #                             "TODAY_VOL" : "TODAY_LOTS", "YESTERDAY_VOLUME": "YESTERDAY_LOTS"}, inplace=True)

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



    col_of_df_return = [c for c in ["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE", "TODAY_LOTS", "CLOSE_MA10",\
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



###################



@mt4_mt5_bp.route('/BGI_All_Symbol_Float2', methods=['GET', 'POST'])
@roles_required()
def BGI_All_Symbol_Float2():

    title = "MT4/MT5 Symbol Float"
    header = "MT4/MT5 Symbol Float"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup("<b>Floating PnL By Symbol for MT4 and MT5.</b><br>"+
                         "Includes B book Groups. <br>"+
                         'Using Live5.group_table where book = "B" for MT4 Groups<br>' +
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
                         "Taking Live prices from Live 1 q Symbols.<br>" + \
                         "For ticks that Live 1 dosn't have, Ticks are taken from Live 2.")




        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename=background_pic("BGI_Symbol_Float"),
                           icon= "",
                           Table_name="MT4/MT5 Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('mt4_mt5.BGI_All_Symbol_Float_ajax2', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(High)']))


# Get BGI Float by Symbol
@mt4_mt5_bp.route('/BGI_All_Symbol_Float_ajax2', methods=['GET', 'POST'])
@roles_required()
def BGI_All_Symbol_Float_ajax2():



    # start_time = datetime.datetime.now()
    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    # Get all the data that is pertainint to MT4 Floating.
    df_mt4 = mt4_Symbol_Float_Data()
    # print("Time taken for MT4 call: {}".format((datetime.datetime.now() - start_time).total_seconds()))
    start_time = datetime.datetime.now()
    df_mt5 = mt5_symbol_float_data()

    df_mt4_average = mt4_symbol_average_data()
    # print("df_mt4_average")
    # print(df_mt4_average)

    # print("Time taken for MT5 Call: {}".format((datetime.datetime.now() - start_time).total_seconds()))

    # print()
    # print(df_mt4.columns)
    # print()
    # print(df_mt5.columns)

    #MT4 #['SYMBOL', 'VOLUME', 'NETVOL', 'REVENUE', 'TODAY_VOL', 'TODAY_REVENUE', 'ASK', 'BID', 'DATETIME', 'YESTERDAY_VOLUME', 'YESTERDAY_REVENUE', 'YESTERDAY_DATE'],
    #MT5 %['SYMBOL', 'FLOATING_LOTS', 'NET_LOTS', 'REVENUE', 'TODAY_LOTS', 'TODAY_REVENUE', 'YESTERDAY_LOTS', 'YESTERDAY_REVENUE', 'YESTERDAY_REBATE', 'YESTERDAY_DATETIME_PULL', 'DATETIME']

    df_mt4.rename(columns={"NETVOL": "NET_LOTS", "VOLUME": "FLOATING_LOTS", "TODAY_VOL" : "TODAY_LOTS", "YESTERDAY_VOLUME": "YESTERDAY_LOTS"}, inplace=True)

    # Clearing up columns that is not needed.
    if "DATETIME" in df_mt5:
        df_mt5.pop("DATETIME")
    if "YESTERDAY_DATETIME_PULL" in df_mt5:
        df_mt5.pop("YESTERDAY_DATETIME_PULL")
    if "YESTERDAY_REBATE" in df_mt5:
        df_mt5.pop("YESTERDAY_REBATE")


    # Concat the MT4 and MT5 Tables
    df_to_table = pd.concat([df_mt4, df_mt5], ignore_index=True)

    # Want to get the ticks that Live 1 don't have from Live 2.
    # Need to do some Clean up for CFDs as well.
    missing_ticks_df = get_live2_ask_bid(list(df_to_table[df_to_table['BID'].isnull()]['SYMBOL'].unique()) + [".DE30"])


    if 'MODIFY_TIME' in missing_ticks_df:
        missing_ticks_df.pop("MODIFY_TIME")
    df_to_table = pd.concat([df_to_table, missing_ticks_df], ignore_index=True)




    # Want to Aggregate the columns accordingly.
    df_to_table = df_to_table.groupby("SYMBOL").agg(
        NET_LOTS=           pd.NamedAgg(column="NET_LOTS", aggfunc="sum"),
        FLOATING_LOTS=      pd.NamedAgg(column="FLOATING_LOTS", aggfunc="sum"),
        REVENUE=            pd.NamedAgg(column="REVENUE", aggfunc="sum"),
        TODAY_LOTS=         pd.NamedAgg(column="TODAY_LOTS", aggfunc="sum"),
        TODAY_REVENUE=      pd.NamedAgg(column="TODAY_REVENUE", aggfunc="sum"),
        BID=                pd.NamedAgg(column="BID", aggfunc="mean"),
        ASK=                pd.NamedAgg(column="ASK", aggfunc="mean"),
        YESTERDAY_LOTS=     pd.NamedAgg(column="YESTERDAY_LOTS", aggfunc="sum"),
        YESTERDAY_REVENUE=  pd.NamedAgg(column="YESTERDAY_REVENUE", aggfunc="sum")   ).reset_index()

    # # pd.set_option('display.max_columns', None)
    # print()
    # # df_to_table = pd.concat([df_mt4, df_mt5], ignore_index=True)
    # print(df_to_table)

    # Want to merge it to compare it with the average of MT4 to compare.
    if "Basesymbol" in df_mt4_average and len(df_mt4_average) > 0:
        df_to_table = df_to_table.merge(df_mt4_average, left_on="SYMBOL", right_on="Basesymbol", how="left")
    else:
        print("Nothing to join on.")


    #print(df_to_table)

    #['SYMBOL', 'FLOATING_LOTS', 'NET_LOTS', 'REVENUE', 'TODAY_LOTS','TODAY_REVENUE', 'ASK', 'BID', 'DATETIME', 'YESTERDAY_LOTS','YESTERDAY_REVENUE', 'YESTERDAY_DATE']

    # print(df_to_table)
    # print()
    yesterday_datetime_pull = ["No YESTERDAY_DATE in df."]
    # We already know the date. No need to carry on with this data.
    if "YESTERDAY_DATE" in df_to_table:

        # Want to get the Unique date for the "Yesterday" date.
        # Want to know the date of the symbol "yesterday" details.
        yesterday_datetime_pull = [c for c in list(df_to_table[df_to_table['YESTERDAY_DATE'].notna()]['YESTERDAY_DATE'].unique()) if c != 0]

        # We have no need for the column anymore.
        df_to_table.pop('YESTERDAY_DATE')


    #end = datetime.datetime.now()
    #print("\nGetting SYMBOL PnL tool[After Query]: {}s\n".format((end - start).total_seconds()))

    datetime_pull = ["No Datetime in df."]   # Default Value
    if "DATETIME" in df_to_table:
        # Get Datetime into string
        df_to_table['DATETIME'] = df_to_table['DATETIME'].apply(lambda x: Get_time_String(x) if isinstance(x, pd.Timestamp) else x)
        #print(df_to_table['DATETIME'])
        datetime_pull =  [c for c in list(df_to_table['DATETIME'][df_to_table['DATETIME'].notnull()].unique()) if c != 0]
        df_to_table.pop('DATETIME')



    # Sort by abs net volume
    df_to_table["ABS_NET"] = df_to_table["NET_LOTS"].apply(lambda x: abs(x))
    df_to_table.sort_values(by=["ABS_NET"], inplace=True, ascending=False)
    df_to_table.pop('ABS_NET')


    # SQL sometimes return values with lots of decimal points.
    # We only want to show afew. Else, it takes up too much screen space.
    # Want to do this for both BID and ASK column
    col_from_exp_to_str = ["BID", "ASK"]
    for c in col_from_exp_to_str:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: "{:2.5f}".format(float(x)) if (isfloat(decimal.Decimal(str(x)).as_tuple().exponent)
                                                                                            and (decimal.Decimal(str(x)).as_tuple().exponent < -5)) else x)

    # Time to fill in the NAs
    df_to_table.fillna("-", inplace=True)



    # Hyperlink this.
    if "YESTERDAY_REVENUE" in df_to_table:
        # Hyperlink it.
        df_to_table["YESTERDAY_REVENUE"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_REVENUE}</a>""".format( \
                                                            url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                            YESTERDAY_REVENUE=profit_red_green(x["YESTERDAY_REVENUE"]) if isfloat(x["YESTERDAY_REVENUE"]) else x["YESTERDAY_REVENUE"] ),
                                                            axis=1)


    # Also want to hyperlink this.
    # Just.. to have more hypterlink. HA ha ha.
    # Haven changed name yet. So it's still names "volume"
    if "YESTERDAY_LOTS" in df_to_table:
        # Hyperlink it.
        df_to_table["YESTERDAY_LOTS"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_LOTS}</a>""".format( \

                                                            url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                            YESTERDAY_LOTS=round(x["YESTERDAY_LOTS"], 2) if isfloat(x["YESTERDAY_LOTS"]) else x["YESTERDAY_LOTS"]),
                                                            axis=1)


    if "TODAY_LOTS" in df_to_table:
        # Want to hyperlink it.
        # Will need to write in '(High)' so that Javascript can pick it up and highlight the cell
        df_to_table["TODAY_LOTS"] = df_to_table.apply(lambda x: '{High_vol}<a style="color:black" href="{url}" target="_blank">{TODAY_LOTS:.2f}</a>'.format( \
                                                        High_vol="(High)" if ('CLOSE_MA10_VOL' in x and x['TODAY_LOTS'] >= x["CLOSE_MA10_VOL"]) else "",
                                                        TODAY_LOTS=x['TODAY_LOTS'],
                                                        url=url_for('analysis.symbol_float_trades',
                                                                    _external=True, symbol=x['SYMBOL'],
                                                                    book="b")),
                                                      axis=1)


    if "FLOATING_LOTS" in df_to_table:
        # Want to hyperlink it.
        df_to_table["FLOATING_LOTS"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{FLOATING_LOTS'
                                                                   '}</a>'.format(\
                                                                        FLOATING_LOTS="{:.2f}".format(x['FLOATING_LOTS']) if isfloat(x['FLOATING_LOTS']) else x['FLOATING_LOTS'],
                                                                        url=url_for('analysis.symbol_float_trades', _external=True, symbol=x['SYMBOL'], book="b")), axis=1)


    # Want to hyperlink it.
    df_to_table["SYMBOL"] = df_to_table["SYMBOL"].apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{symbol}</a>'.format(symbol=x,
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x, book="b")))


   # # # Want to add colors to the words.
    # Want to color the REVENUE
    cols = ["REVENUE", "TODAY_REVENUE", "NET_LOTS"]
    for c in cols:
        if c in df_to_table:
            df_to_table[c] = df_to_table[c].apply(lambda x: """{c}""".format(c=profit_red_green(x) if isfloat(x) else x))

    #Rename the VOLUME to LOTs
    # df_to_table.rename(columns={"NETVOL": "NET_LOTS", "VOLUME": "FLOATING_LOTS",
    #                             "TODAY_VOL" : "TODAY_LOTS", "YESTERDAY_VOLUME": "YESTERDAY_LOTS"}, inplace=True)

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



    col_of_df_return = [c for c in ["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE", "TODAY_LOTS", "CLOSE_MA10_VOL",\
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



#####################



# To Query for all open trades by a particular symbol
# Shows the closed trades for the day as well.
@mt4_mt5_bp.route('/ABook_BGI', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Dealing"])
def ABook_BGI():

    title = "A Book BGI"
    header =  "A Book BGI"
    description = Markup("A Book BGI MT4 and MT5")

    return render_template("Wbwrk_Multitable_Borderless.html", backgroud_Filename=background_pic("ABook_BGI"), icon="",
                           Table_name={"A Book BGI": "H1"},
                           title=title, setinterval=60,
                           ajax_url=url_for('mt4_mt5.A_Book_symbols_float_trades_ajax', _external=True),
                           header=header,
                           description=description, no_backgroud_Cover=True,
                           replace_words=Markup(["Today"])) #setinterval=60,


@mt4_mt5_bp.route('/A_Book_symbols_float_trades_ajax', methods=['GET', 'POST'])
@roles_required(["Risk", "Risk_TW", "Admin", "Dealing"])
def A_Book_symbols_float_trades_ajax(update_tool_time=0):    # To upload the Files, or post which trades to delete on MT5

    mismatch_count = [10,15]

    # The code is in aaron database, saved as a procedure.
    curent_result = Query_SQL_db_engine("call aaron.mt4_ABook_Position()")

    # Get the MT5 ABook data from SQL, using the MT5 lib.
    #SQL Stored Procedure
    mt5_result = mt5_ABook_data()

    df_mt4_postion = pd.DataFrame(data=curent_result)

    # Need to rename some Columns.
    df_mt5_postion = pd.DataFrame(data=mt5_result)
    df_mt5_postion = df_mt5_postion.rename(columns={"baseSymbol": "SYMBOL", "Profit_usd" : "MT5 REVENUE", "Net_Volume":"MT5 Net Lots"})
    df_mt5_postion = df_mt5_postion[["SYMBOL", "MT5 Net Lots", "MT5 REVENUE"]]


    df_postion = df_mt4_postion.merge(df_mt5_postion, on="SYMBOL")
    # Need to recalculate the Discrepancy as we need to add in MT5 Codes as well.
    df_postion["Discrepancy"] = df_postion["Lp_Net_lot"] - (df_postion["MT4_Net_lot"] + df_postion["MT5 Net Lots"])

    #print(df_postion)

    # Variables to return.
    Play_Sound = 0  # To play sound if needed



    # curent_result[10]["Discrepancy"] = 0.1  # Artificially induce a mismatch
    # print("request.method: {}".format(request.method))
    # print("Len of request.form: {}".format(len(request.form)))

    ## Need to check if the post has data. Cause from other functions, the POST details will come thru as well.
    if request.method == 'POST' and len(request.form) > 0:    # If the request came in thru a POST. We will get the data first.
        #print("Request A Book Matching method: POST")

        # This will sometimes cause everything in the dict to become list.
        post_data = dict(request.form)  # Want to do a copy.



        # Check if we need to send Email
        # Need to check if it's a list or a string.
        Send_Email_Flag = 0
        if "send_email_flag" in post_data:
            if isinstance(post_data['send_email_flag'], str) and isfloat(post_data['send_email_flag']):
                Send_Email_Flag = int(post_data["send_email_flag"])
            elif  isinstance(post_data['send_email_flag'], list) and len(post_data['send_email_flag']) > 0 and isfloat(post_data['send_email_flag'][0]):
                Send_Email_Flag = int(post_data["send_email_flag"][0])
            else:
                Send_Email_Flag = 0

        # Send_Email_Flag =  int(post_data["send_email_flag"]) if ("send_email_flag" in post_data) \
        #                                                            and (isinstance(post_data['send_email_flag'], str)
        #                                                                 and isfloat(post_data['send_email_flag'])) else 0


        # Check for the past details.
        # Should be stored in Javascript, and returned back Via post.

        Past_Details = []
        if "MT4_LP_Position_save" in post_data:
            if isinstance(post_data['MT4_LP_Position_save'], str) and  is_json(post_data["MT4_LP_Position_save"]):
                Past_Details = json.loads(post_data["MT4_LP_Position_save"])
            elif isinstance(post_data['MT4_LP_Position_save'], list) and len(post_data['MT4_LP_Position_save']) > 0 and  is_json(post_data["MT4_LP_Position_save"][0]):
                Past_Details = json.loads(post_data["MT4_LP_Position_save"][0])
            else:
                Past_Details = []

        #print(post_data["MT4_LP_Position_save"])
        # print("Past_Details")
        # print(Past_Details)

        # Past_Details = json.loads(post_data["MT4_LP_Position_save"]) if ("MT4_LP_Position_save" in post_data) \
        #                                                                    and (isinstance(post_data['MT4_LP_Position_save'], str)) \
        #                                                                    and is_json(post_data["MT4_LP_Position_save"]) \
        #                                                                     else []

        # To revert back to a normal Symbol string, instead of a URL.
        df_past_details = pd.DataFrame(Past_Details)


        # print("past details")
        # print(df_past_details)

        if "SYMBOL" in df_past_details:
            df_past_details["SYMBOL"] = df_past_details["SYMBOL"].apply(lambda x: BeautifulSoup(x, features="lxml").a.text \
                                                                    if BeautifulSoup(x, features="lxml").a != None else x)
        Past_Details = df_past_details.to_dict("record")

        # If we want to send all the total position
        send_email_total = int(post_data["send_email_total"][0]) if ("send_email_total" in post_data) \
                                                                   and (isinstance(post_data['send_email_total'], list)) else 0



        #print("Past Details: {}".format(Past_Details))
        # To Calculate the past (Previous result) Mismatches
        Past_discrepancy = dict()
        for past in Past_Details:
            if "SYMBOL" in past \
                    and "Discrepancy" in past.keys() \
                    and past["Discrepancy"] != 0:    # If the keys are there.
                    Past_discrepancy[past["SYMBOL"]] = past["Mismatch_count"] if "Mismatch_count" in past else 1  # Want to get the count. Or raise as 1.

        # # Using pandas
        # # Get dataframe of only those that has Discrepancy for the past records
        # df_past_discrepancy = pd.DataFrame()
        # if all(a in df_past_details for a in ["SYMBOL", "Discrepancy"]):
        #     df_past_discrepancy = df_past_details[df_past_details["Discrepancy"] != 0][["SYMBOL", "Discrepancy"]]


        # # #To Artificially induce a mismatch
        # # curent_result[0]["Discrepancy"] = 0.01
        # # curent_result[1]["Discrepancy"] = 0.01
        # # curent_result[2]["Discrepancy"] = 0.01


        # To tally off with current mismatches. If there are, add 1 to count. Else, Zero it.
        for d in curent_result:
            if "Discrepancy" in d.keys():
                if d["Discrepancy"] != 0:   # There are mismatches Currently.
                    d["Mismatch_count"] = 1 if d['SYMBOL'] not in Past_discrepancy else Past_discrepancy[d['SYMBOL']] + 1
                else:
                    d["Mismatch_count"] = 0


        # # Using pandas
        # # Get dataframe of only those that has Discrepancy for the current records
        # df_current_discrepancy = pd.DataFrame()
        # if all(a in df_postion for a in ["SYMBOL", "Discrepancy"]):
        #     df_current_discrepancy = df_postion[df_postion["Discrepancy"] != 0]
        #
        # # Want to get all the mismatches using pandas
        # Notify_Mismatch = df_current_discrepancy.to_dict("record")   # Need to rearrange the column names
        # Current_discrepancy = list(df_current_discrepancy["SYMBOL"])       # Get all the Mimatch Symbols only


        # Want to get all the mismatches.
        Notify_Mismatch = [d for d in curent_result if d['Mismatch_count'] != 0 ]

        Current_discrepancy = [d["SYMBOL"] for d in Notify_Mismatch]        # Get all the Mimatch Symbols only


        #print("Current Discrepency: {}".format(Current_discrepancy))

        if (send_email_total == 1): # for sending the total position.

            email_table_html = Array_To_HTML_Table(list(curent_result[0].keys()),
                                                            [list(d.values()) for d in curent_result])

            email_title = "ABook Position(Total, with {} mismatches.)".format(len(Notify_Mismatch)) \
            if len(Notify_Mismatch) > 0 else "ABook Position(Total)"

            async_send_email(EMAIL_LIST_ALERT, [],
                             email_title,
                             Email_Header + "Hi, <br><br>Kindly find the total position of the MT4/LP Position. <br> "
                             + email_table_html + "<br>Thanks,<br>Aaron" + Email_Footer, [])
        else:
            if Send_Email_Flag == 1:  # Only when Send Email Alert is set, we will
                async_update_Runtime(app=current_app._get_current_object(), Tool='MT4/LP A Book Check')     # Want to update the runtime table to ensure that tool is running.



                #
                # # If there are mismatches, first thing to do is to update CFH. All trades.
                # # Some older trades might have been closed.
                # if any([d["Mismatch_count"] in cfh_soap_query_count for d in Notify_Mismatch]):
                #     chf_fix_details_ajax()  # Want to update CFH Live Trades.
                #     #TODO: Update Vantage Live trades too, if possible.

                    #CFH_Live_Position_ajax(update_all=1)    # Want to update all trades from CFH
                    #print("Mismatch. Will Send SOAP to refresh all trades.")


            Tele_Message = "<b>MT4/LP Position</b> \n\n"  # To compose Telegram outgoing message
            email_html_body = "Hi, <br><br>";
            Email_Title_Array = []

            # If there are mismatch count that are either mismatch_count_1 or mismatch_count_2, we will send the email.
            # if any([ d["Mismatch_count"] in mismatch_count for d in Notify_Mismatch]):    # If there are to be notified.
            #
            #     Play_Sound += 1  # Raise the flag to play sound.
            #     Notify_mismatch_table_html = Array_To_HTML_Table(list(Notify_Mismatch[0].keys()), [list(d.values()) for d in Notify_Mismatch])
            #     Email_Title_Array.append("Mismatch")
            #     email_html_body +=  "There is a mismatch for A-Book LP/MT4 trades.<br>{}".format(Notify_mismatch_table_html)
            #
            #
            #     # Want to find the potential mismatch trades from MT4 and Bridge
            #     ##bridge_trades = Mismatch_trades_bridge(symbol=Current_discrepancy, hours=7, mins=16)
            #     ##mt4_trades = Mismatch_trades_mt4(symbol=Current_discrepancy, hours=7, mins=16)
            #
            #     # Bridge data is in GMT.
            #     # Mins would take the max of mismatch_count + 1 for good measure.
            #     bridge_trades = Mismatch_trades_bridge(symbol=Current_discrepancy, hours=8, mins=max(mismatch_count) + 1)
            #
            #     # MT4 Live 1 server difference timing.
            #     # Mins would take the max of mismatch_count + 1 for good measure.
            #     live1_server_difference = session[
            #         "live1_sgt_time_diff"] if "live1_sgt_time_diff" in session else get_live1_time_difference()
            #     mt4_trades = Mismatch_trades_mt4(symbol=Current_discrepancy, hours=live1_server_difference, mins=max(mismatch_count) + 1)
            #
            #     # Converts it to a HTML table if there are trades. Else, show that there is no trades found.
            #     bridge_trades_html_table = Array_To_HTML_Table(Table_Header = bridge_trades[0], Table_Data=bridge_trades[1]) \
            #         if len(bridge_trades[1]) > 0 else "- No Trades Found for that time perid.\n"
            #
            #     mt4_trades_html_table = Array_To_HTML_Table(Table_Header=mt4_trades[0], Table_Data=mt4_trades[1]) \
            #         if len(mt4_trades[1]) > 0 else "- No Trades Found for that time perid.\n"
            #
            #     email_html_body += "<br><b><u>MT4 trades</u></b><br> - Time is Approx<br> - CMD < 2 trades only.<br>{mt4_table}<br><br><b><u>Bridge(SQ) trades</u></b> around that time:<br>{bridge_table}<br>".format(
            #         mt4_table=mt4_trades_html_table,bridge_table=bridge_trades_html_table)
            #
            #     # print(Notify_Mismatch)
            #     Tele_Message += "<pre>{} Mismatch</pre>\n {}".format(len(Current_discrepancy), " ".join(["{}: {} Lots, {} Mins.\n".format(c["SYMBOL"], c["Discrepancy"], c["Mismatch_count"]) for c in Notify_Mismatch]))

            Cleared_Symbol = [sym for sym,count in Past_discrepancy.items() if (sym not in Current_discrepancy) and count >= min(mismatch_count) ]    # Symbol that had mismatches and now it's been cleared.

            # If Mismatchs have been cleared.
            if len(Cleared_Symbol) > 0:     # There are symbols that have been cleared
                # Get the Symbol data from current SQL return data.


                Cleared_Symbol_data = [d for d in curent_result if "SYMBOL" in d and d["SYMBOL"] in Cleared_Symbol]
                # Create the HTML Table
                Notify_cleared_table_html = Array_To_HTML_Table(list(Cleared_Symbol_data[0].keys()),
                                                                 [list(d.values()) for d in Cleared_Symbol_data])
                Email_Title_Array.append("Cleared")
                email_html_body += "The Following symbol/s mismatch have been cleared: {} <br> {}".format(", ".join(Cleared_Symbol), Notify_cleared_table_html)
                Tele_Message += "{} Cleared: <b>{}</b>\n".format(len(Cleared_Symbol), ", ".join(Cleared_Symbol))

            #
            #
            # if Send_Email_Flag == 1 and len(Email_Title_Array) > 0:    # If there are things to be sent, we determine by looking at the title array
            #     api_update_details = json.loads(LP_Margin_UpdateTime())  # Want to get the API/LP Update time.
            #
            #     email_html_body +=  "The API/LP Update timings:<br>" + Array_To_HTML_Table(
            #         list(api_update_details[0].keys()), [list(api_update_details[0].values())], ["Update Slow"])
            #     email_html_body += "This Email was generated at: SGT {}.<br><br>Thanks,<br>Aaron".format(
            #         datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            #
            #     #print(EMAIL_LIST_ALERT)
            #
            #     # Send the email
            #     async_send_email(EMAIL_LIST_ALERT, [], "A Book Position ({}) ".format("/ ".join(Email_Title_Array)),
            #            Email_Header + email_html_body + Email_Footer, [])
            #
            #     # Send_Email(EMAIL_LIST_ALERT, [], "A Book Position ({}) ".format("/ ".join(Email_Title_Array)), Email_Header + email_html_body + Email_Footer, [])
            #
            #     # Want to send to telegram the timing that the API was updated.
            #     api_update_time = api_update_details[0] if len(api_update_details) else {}
            #     api_update_str = "\n<pre>Update time</pre>\n" + "\n".join(["{k} : {d}".format(k=k, d=d.replace("<br>", " ")) for k,d in api_update_time.items()]) \
            #                             if len(api_update_details) else ""
            #
            #     Tele_Message += api_update_str
            #
            #     # Send the Telegram message.
            #     async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Message, TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)

        # '[{"Vantage_Update_Time": "2019-09-17 16:54:20", "BGI_Margin_Update_Time": "2019-09-17 16:54:23"}]'

    # To add the symbol Link.For hyperlink to the A book Symbol page that shows symbol trades.
    # Will use Beautiful soup to parse it back to symbols later when recieved as POST

    #print(df_postion)
    df_postion["SYMBOL"] = df_postion.apply(lambda x: Symbol_Trades_url(symbol=x["SYMBOL"], book="a"), axis = 1)


    col_needed = [ "SYMBOL", "Vantage_lot", "CFH_lot", "GP_lot", "API_lot", "Offset_lot", "Lp_Net_lot", "MT4_Net_lot", "MT4_Revenue", "Discrepancy", "Mismatch_count"]

    # If there is no lots in CFH at all, we don't need to show the column
    if "CFH_lot" in df_postion and df_postion["CFH_lot"].abs().sum() == 0 :
        #print(df_postion["CFH_lot"].abs().sum())
        col_needed.remove("CFH_lot")
        #df_postion["CFH_lot"] = 1

    col_to_use = [c for c in col_needed if c in df_postion]     # Just in case the column is not in the df.

    # Arrange it all in the correct position
    df_postion = df_postion[col_to_use]

    # Want to color the Revenue column
    if "MT4_Revenue" in df_postion:
        df_postion["MT4_Revenue"] = df_postion["MT4_Revenue"].apply(profit_red_green)


    curent_result = df_postion.to_dict("record")


    #print("Current Results: {}".format(curent_result))
    return_result = {"current_result":curent_result, "Play_Sound": Play_Sound}   # End of if/else. going to return.

    #print(return_result)
    return json.dumps({"H1" : curent_result})



