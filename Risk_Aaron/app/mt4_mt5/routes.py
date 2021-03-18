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
                           title=title, ajax_url=url_for('mt4_mt5_bp.BGI_MT5_Symbol_Float_ajax', _external=True),
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

    if check_session_live1_timing() == True and "yesterday_pnl_by_symbol" in session \
            and  len(session["yesterday_pnl_by_symbol"]) > 0:
        # From "in memory" of session
        #print(session)
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
