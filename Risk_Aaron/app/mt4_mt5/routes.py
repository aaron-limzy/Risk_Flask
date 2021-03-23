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
                         "Taking Live prices from Live 1 q Symbols")




        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Symbol_Float.html", backgroud_Filename=background_pic("BGI_Symbol_Float"),
                           icon= "",
                           Table_name="MT4/MT5 Symbol Float (B 📘)", \
                           title=title, ajax_url=url_for('mt4_mt5.BGI_All_Symbol_Float_ajax', _external=True),
                           header=header, setinterval=15,
                           tableau_url=Markup(symbol_float_tableau()),
                           description=description, no_backgroud_Cover=True, replace_words=Markup(['(Client Side)']))


# Get BGI Float by Symbol
@mt4_mt5_bp.route('/BGI_All_Symbol_Float_ajax', methods=['GET', 'POST'])
@roles_required()
def BGI_All_Symbol_Float_ajax():


    #start = datetime.datetime.now()
    # TODO: Only want to save during trading hours.
    # TODO: Want to write a custom function, and not rely on using CFH timing.
    if not cfh_fix_timing():
        return json.dumps([[{'Update time' : "Not updating, as Market isn't opened. {}".format(Get_time_String())}]])

    # Get all the data that is pertainint to MT4 Floating.
    df_mt4 = mt4_Symbol_Float_Data()
    df_mt5 = mt5_symbol_float_data()

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


    # ["SYMBOL", "NET_LOTS", "FLOATING_LOTS", "REVENUE", "TODAY_LOTS", \
    #  "TODAY_REVENUE", "BID", "ASK", "YESTERDAY_LOTS", "YESTERDAY_REVENUE"]
    # Concat the MT4 and MT5 Tables
    df_to_table = pd.concat([df_mt4, df_mt5], ignore_index=True)
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

    # pd.set_option('display.max_columns', None)
    # print()
    # df_to_table = pd.concat([df_mt4, df_mt5], ignore_index=True)
    #print(df_to_table)
    # print()
    # print(df_to_table.columns)

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
    if "YESTERDAY_LOTS" in df_to_table:
        # Hyperlink it.
        df_to_table["YESTERDAY_LOTS"] = df_to_table.apply(lambda x: """<a style="color:black" href="{url}" target="_blank">{YESTERDAY_LOTS}</a>""".format( \
                                                            url=url_for('analysis.symbol_closed_trades', _external=True, symbol=x["SYMBOL"], book="b", days=-1),
                                                            YESTERDAY_LOTS=x["YESTERDAY_LOTS"]),
                                                            axis=1)


    # Want to hyperlink it.
    df_to_table["SYMBOL"] = df_to_table["SYMBOL"].apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{symbol}</a>'.format(symbol=x,
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x, book="b")))


    if "TODAY_LOTS" in df_to_table:
    # Want to hyperlink it.
        df_to_table["TODAY_LOTS"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{TODAY_LOTS}</a>'.format(TODAY_LOTS=x['TODAY_LOTS'],
                                                                    url=url_for('analysis.symbol_float_trades', _external=True, symbol=x['SYMBOL'], book="b")), axis=1)
    if "FLOATING_LOTS" in df_to_table:
        # Want to hyperlink it.
        df_to_table["FLOATING_LOTS"] = df_to_table.apply(lambda x: '<a style="color:black" href="{url}" target="_blank">{FLOATING_LOTS}</a>'.format(\
                                                                        FLOATING_LOTS="{:.2f}".format(x['FLOATING_LOTS']),
                                                                        url=url_for('analysis.symbol_float_trades', _external=True, symbol=x['SYMBOL'], book="b")), axis=1)

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