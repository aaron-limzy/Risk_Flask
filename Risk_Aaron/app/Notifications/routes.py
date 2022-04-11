from flask import Blueprint, render_template, Markup, url_for, request, session, current_app, flash, redirect
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from sqlalchemy import text
from app.extensions import db, excel

import plotly
import plotly.graph_objs as go
import plotly.express as px

import pandas as pd
import numpy as np
import json

from app.decorators import roles_required
from app.Notifications.forms import *
from app.Notifications.table import *


from ..Risk_Tools_Config import email_flag_fun
import emoji
import flag

from app.Swaps.forms import UploadForm

from flask_table import create_table, Col

import requests

import pyexcel
from werkzeug.utils import secure_filename

from Helper_Flask_Lib import *

import decimal

notifications_bp = Blueprint('notifications_bp', __name__)


# Want to check and close off account/trades.
# Main page.
@notifications_bp.route('/Client_No_Symbol_Trades', methods=['GET', 'POST'])
@roles_required()
def Client_No_Symbol_Trades():

    title = "Client Close Symbol"
    header = title

    description = Markup('Check if client has closed off (to zero) any <b>particular symbols</b>.<br>' +\
                          'This tool is <b>Post-Fix Specific.</b>' +\
                         'Sql Table used: <u>aaron.client_zero_position</u><br><br>' +\
                         'This tool will only check particular symbols that has been set up in the SQL table.<br>' +\
                         'When the client closes all trades on that particular symbol, a telegram (only) message will be sent out.<br>'+\
                         'Do note, this tool will only send a notification once.<br>If the client re-opens the same symbol, it will no longer be tracked.')

        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/pattern8.png', Table_name="Client Net Volume.", \
                           title=title, ajax_url=url_for('notifications_bp.Client_No_Trades_ajax', _external=True), header=header, setinterval=60,
                           no_backgroud_Cover=True,
                           description=description, replace_words=Markup(["Today"]))
#"Client Symbol No Open Trades" : Client_No_Trades_ajax
# To notify if client has any open trades on a particular symbol.
# This is the ajax.
@notifications_bp.route('/Client_No_Trades_ajax', methods=['GET', 'POST'])
@roles_required()
def Client_No_Trades_ajax(update_tool_time=1):


    raw_sql_statement = """SELECT
	c.LIVE as LIVE, c.LOGIN as LOGIN, c.SYMBOL as SYMBOL, c.DATETIME as `DATETIME`, COALESCE(t.LOTS,0) as LOTS,
	 COALESCE(t.REVENUE,0) as REVENUE, COALESCE(t.`GROUP`,'-') as `GROUP`
    FROM
        ( SELECT LIVE, LOGIN, SYMBOL, DATETIME FROM aaron.client_zero_position WHERE live = '{live}' AND active='1')AS c
    LEFT JOIN(
        SELECT LOGIN, SUM(PROFIT+SWAPS) AS REVENUE, SUM(VOLUME)*0.01 as LOTS, SYMBOL, `GROUP`
        FROM live{live}.mt4_trades
        WHERE login IN( SELECT LOGIN FROM aaron.client_zero_position WHERE live = '{live}' and active='1' )
        AND CLOSE_TIME = '1970-01-01 00:00:00' AND CMD < 2
        GROUP BY LOGIN, SYMBOL
    )AS t ON( t.LOGIN = c.LOGIN AND t.symbol = c.symbol)"""

    # run thru for all live.
    sql_statement = " UNION ".join([raw_sql_statement.format(live=l) for l in [1,2,3,5]])

    sql_statement = sql_statement.replace("\n", " ").replace("\t", " ")
    #print(sql_statement)

    sql_query = text(sql_statement)
    return_val = Query_SQL_db_engine(sql_query)


    df = pd.DataFrame(return_val)
    #print(df)

    if len(df) == 0: # There is no return.
        return_val = [{"RESULT": "No SQL Returns. Time Now: {}".format(time_now())}]

    #default return val.
    else:
        # To print out the datetime in str format.
        df["DATETIME"] = df["DATETIME"].apply(lambda x: "{}".format(x))

        df_no_lots = df[df["LOTS"] == 0]    # Want to get those that have no lots.

        # print(df_no_lots)

        if len(df_no_lots) > 0: # If there is, we assume the client has closed the position.
            # Want to embed the LOGIN link.

            df_no_lots["LOGIN_URL"] = df_no_lots.apply(
                lambda x: live_login_analysis_url_External(Live=x["LIVE"], Login=x["LOGIN"]), axis=1)

            # Telegram
            sql_update_values = df_no_lots[["LIVE", "LOGIN_URL", "SYMBOL", "DATETIME"]].values.tolist()

            # For the empty Data
            telegram_data = df_no_lots[["LIVE", "LOGIN_URL", "LOTS", "SYMBOL"]].values.tolist()
            telegram_data = [["{}".format(t) for t in td] for td in telegram_data]
            telegram_data = "\n".join([" | ".join(t) for t in telegram_data])


            # For the non-empty Data
            df_lots =  df[df["LOTS"] != 0]
            if len(df_lots)>0:
                telegram_data_with_lots = df_lots[["LOGIN_URL", "LOGIN", "LOTS", "SYMBOL"]].values.tolist()
                telegram_data_with_lots = [["{}".format(t) for t in td] for td in telegram_data_with_lots]
                telegram_data_with_lots = "\n".join([" | ".join(t) for t in telegram_data_with_lots])

                telegram_data_with_lots_text = "\n\n-----------------------------------------\nBelow are the currently monitored symbol.\n"
                telegram_data_with_lots_text += "\n<pre>Live | Login | Lots | Symbol</pre>\n"
                telegram_data_with_lots_text += telegram_data_with_lots + "\n"

            else:
                telegram_data_with_lots_text = "" # Nothing to show for it.

            print(telegram_data)

            async_Post_To_Telegram(BGI_MONITOR_TELEGRAM_TOKEN,
                    "<b>Client position closed.</b>\n\n<pre>Live | Login | Lots | Symbol</pre>\n{telegram_data} ".format(telegram_data=telegram_data) + \
                       "\n\nThese Symbols no longer be tracked." + \
                    "{telegram_data_with_lots_text} ".format(telegram_data_with_lots_text=telegram_data_with_lots_text),
                                   [TELEGRAM_ALERT_GROUP_CHAT], Parse_mode=telegram.ParseMode.HTML)

            # Email # TODO

            # async_send_email(To_recipients=EMAIL_LIST_BGI, cc_recipients=[],
            #           Subject="USOil Below {} Dollars.".format(USOil_Price_Alert),
            #           HTML_Text="""{Email_Header}Hi,<br><br>USOil Price is at {usoil_mid_price}, and it has dropped below {USOil_Price_Alert} USD. <br>
            #                       The following is the C output. <br><br>{c_output}<br><br>
            #                     <br><br>This Email was generated at: {datetime_now} (SGT)<br><br>Thanks,<br>Aaron{Email_Footer}""".format(
            #               Email_Header = Email_Header, USOil_Price_Alert = USOil_Price_Alert, usoil_mid_price = usoil_mid_price, c_output=output, datetime_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            #               Email_Footer=Email_Footer), Attachment_Name=[])

            sql_update_values_2 = df_no_lots[["LIVE", "LOGIN", "SYMBOL", "DATETIME"]].values.tolist()
            # Add in quotes to make it into a string
            sql_update_values_2 = [["'{}'".format(s) for s in s_list] for s_list in sql_update_values_2]

            for s in range(len(sql_update_values_2)):
                sql_update_values_2[s].append("'0'")
                #sql_update_values[s].append("NOW()")

            # Put values into brackets, to be ready for SQL insert.
            sql_update_values_2 = " , ".join([" ({}) ".format(" , ".join(s)) for s in sql_update_values_2])

            sql_insert = """INSERT INTO aaron.client_zero_position (live, login, symbol, datetime, active) VALUES {values} 
                ON DUPLICATE KEY UPDATE active = VALUES(active)""".format(values=sql_update_values_2)
            sql_insert = text(sql_insert)  # To make it to SQL friendly text.

            raw_insert_result = db.engine.execute(sql_insert)

        print(df)
        df["REVENUE"] = df["REVENUE"].apply(profit_red_green)
        return_val = df[["LIVE", "LOGIN", "SYMBOL", "LOTS", "REVENUE", "GROUP"]].to_dict("record")

    # # # Need to update Run time on SQL Update table.
    if update_tool_time == 1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="Client Symbol No Open Trades")

    return json.dumps(return_val)



# Want to insert into table.
# From Flask.form.
# Will insert into aaron.client_zero_position
@notifications_bp.route('/Client_No_Trades_include', methods=['GET', 'POST'])
@roles_required()
def Include_Client_No_Trades():

    title = "Client No Trades [Include Client]"
    header =  Markup("Client_No_Trades [Include]")

    description = Markup(
        """<b>To Include/delete from the running tool to check if client has open trades with a particular symbol</b>
        <br>Will add/delete account and symbol into <span style="color:green"><b>aaron.client_zero_position</b></span>.<br>
        Will send a notification once, when client has no more position on certain symbol/s""")

    form = Client_No_Trade_Form()

    form.validate_on_submit()
    if request.method == 'POST' and form.validate_on_submit():
        Live = form.Live.data  # Get the Data.
        Login = form.Login.data
        Symbol = form.Symbol.data

        sql_insert = """INSERT INTO  aaron.`client_zero_position` (`Live`, `Login`, `symbol`, `active`, `datetime`) VALUES
            ('{Live}','{Account}','{Symbol}', '1', now()) ON DUPLICATE KEY UPDATE `active`=VALUES(`active`) """.format(Live=Live, Account=Login, Symbol=Symbol)
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")

        print(sql_insert)
        db.engine.execute(text(sql_insert))  # Insert into DB
        flash("Live: {live}, Login: {login} Symbol: {Symbol} has been added to aaron.`client_zero_position`.".format(live=Live, login=Login, Symbol=Symbol))
        form = Client_No_Trade_Form() # Get a new, empty form.

    table = Delete_client_No_Trade_Table_fun()

    #Scissors backgound. We do not want it to cover. So.. we want the picture to be repeated.
    return render_template("General_Form.html", title=title, header=header, table=table, form=form,
                           description=description, backgroud_Filename='css/pattern8.png', no_backgroud_Cover=True)



# Want to return the table that will be showing the list of client included in the tool.

def Delete_client_No_Trade_Table_fun():
    # Need to do a left join. To fully pull out the risk_autocut_include Logins.
    # Some Logins might not even be on the server.
    sql_query = """SELECT Live, Login, Symbol FROM aaron.`client_zero_position` 
                    WHERE active=1"""

    #sql_query_array = [raw_sql.format(Live=l) for l in [1,2,3,5]]   # Want to loop thru all the LIVE server

    #sql_query = " UNION ".join(sql_query_array) + " ORDER BY Live, Login " # UNION the query all together.
    collate = query_SQL_return_record(text(sql_query))


    if len(collate) == 0:   # There is no data.
        empty_table = [{"Result": "There are currently no single account included in the autocut. There might still be Groups tho."}]
        table = create_table_fun(empty_table, additional_class=["basic_table", "table", "table-striped",
                                                                "table-bordered", "table-hover", "table-sm"])
    else:
        # Live Account and other information of users that are in the Risk AutoCut Group.
        df_data = pd.DataFrame(collate)
        # df_data["Equity_limit"] = df_data["Equity_limit"].apply(lambda x: "{:,.0f}".format(x) if type(x) == np.float64 else "{}".format(x))
        # df_data["Balance"] = df_data["Balance"].apply(lambda x: "{:,.2f}".format(x) if type(x) == np.float64 else "{}".format(x))
        # df_data["Credit"] = df_data["Credit"].apply(lambda x: "{:,.2f}".format(x) if type(x) == np.float64 else "{}".format(x))
        # df_data["Equity"] = df_data["Equity"].apply(lambda x: "{:,.2f}".format(x) if type(x) == np.float64 else "{}".format(x))

        table = Delete_Client_No_Trades_Table(df_data.to_dict("record"))
        table.html_attrs = {"class": "basic_table table table-striped table-bordered table-hover table-sm"}
    return table


# To remove the account from the tools.
@notifications_bp.route('/Remove_Risk_Autocut_User/<Live>/<Login>/<Symbol>', methods=['GET', 'POST'])
@roles_required()
def Delete_Risk_Autocut_Include_Button_Endpoint(Live="", Login="", Symbol=""):

    # # Write the SQL Statement and Update to disable the Account monitoring.
    sql_update_statement = """UPDATE aaron.client_zero_position SET active='0' WHERE Live='{Live}' \
                        AND Login='{Login}' AND Symbol='{Symbol}'""".format(Live=Live, Login=Login, Symbol=Symbol)
    sql_update_statement = sql_update_statement.replace("\n", "").replace("\t", "")
    # print(sql_update_statement)
    sql_update_statement=text(sql_update_statement)
    result = db.engine.execute(sql_update_statement)

    flash("Live:{Live}, Login: {Login}, Symbol: {Symbol} has been set to inactive in aaron.client_zero_position".format(Live=Live,Login=Login, Symbol=Symbol))
    #print("Request URL: {}".format(redirect(request.url)))
    return redirect(request.referrer)





@notifications_bp.route('/Large_volume_Login', methods=['GET', 'POST'])
@roles_required()
def Large_volume_Login():
    title = "Large Volume Login"
    header = "Large Volume Login"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    description = Markup("<b>Large Volume Login</b>- Details are on Client side.<br>- Profits has been converted to USD<br><br>" + \
                         "SQL Table: aaron.`Large_Volume_Login`<br>")

    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/checked_1.png',
                           Table_name="Large Volume Login", title=title,
                           ajax_url=url_for('notifications_bp.Large_volume_Login_Ajax', _external=True), header=header,
                           setinterval=120,
                           no_backgroud_Cover=True,
                           description=description, replace_words=Markup(["Today"]))


# Insert into aaron.BGI_Float_History_Save
# Will select, pull it out into Python, before inserting it into the table.
# Tried doing Insert.. Select. But there was desdlock situation..
@notifications_bp.route('/Large_volume_Login_Ajax', methods=['GET', 'POST'])
@roles_required()
def Large_volume_Login_Ajax(update_tool_time=1):


    if not cfh_fix_timing():
        return json.dumps([{'Update time': "Not updating, as Market isn't opened. {}".format(Get_time_String())}])

    alert_levels = [50, 100, 200, 300, 400, 500,1000,10000, 50000,100000,1000000]

    # Want to reduce the query overheads. So try to use the saved value as much as possible.
    # server_time_diff_str = session["live1_sgt_time_diff"] if "live1_sgt_time_diff" in session \
    #     else "SELECT RESULT FROM `aaron_misc_data` where item = 'live1_time_diff'"


    raw_sql_statement = """ SELECT X.*, coalesce(L.lots, 0) as `RECORDED LOTS`  FROM (SELECT '{Live}' as LIVE, 
                LOGIN,  
                `COUNTRY`, `GROUP`,
                SUM(`TOTAL LOTS`) as `TOTAL LOTS`, SUM(`OPENED LOTS`) as `OPENED LOTS`, 
                SUM(`CLOSED LOTS`) AS `CLOSED LOTS`,
                SUM(`FLOATING LOTS`) as `FLOATING LOTS`,
                SUM(`CLOSED PROFIT`) AS `CLOSED PROFIT`, 
                SUM(`FLOATING PROFIT`) AS `FLOATING PROFIT`, 
                ROUND(SUM( coalesce(REBATE,0) * `TOTAL LOTS`)) AS `REBATE`
                    
                FROM (SELECT t.LOGIN AS LOGIN, 
                t.SYMBOL, 
                SUM(CASE WHEN OPEN_TIME >= NOW()- INTERVAL 1 DAY THEN VOLUME*0.01 ELSE 0 END) as 'OPENED LOTS',
                SUM(CASE WHEN CLOSE_TIME >= NOW()- INTERVAL 1 DAY THEN VOLUME*0.01 ELSE 0 END) as 'CLOSED LOTS',
                SUM(CASE WHEN CLOSE_TIME = "1970-01-01 00:00:00" THEN VOLUME*0.01 ELSE 0 END) as 'FLOATING LOTS',
                sum(VOLUME)*0.01 as `TOTAL LOTS`,
                ROUND(SUM(CASE 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN t.PROFIT+t.SWAPS 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)/7.78 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND t.CLOSE_TIME != '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) 
                    AS `CLOSED PROFIT`,
                ROUND(SUM(CASE 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'USD') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN t.PROFIT+t.SWAPS 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'HKD') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)/7.78 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'EUR') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'EURUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'GBP') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'GBPUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'NZD') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'NZDUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'AUD') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)*(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'AUDUSD' ORDER BY TIME DESC LIMIT 1) 
                    WHEN (t.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE CURRENCY = 'SGD') AND t.CLOSE_TIME = '1970-01-01 00:00:00') THEN (t.PROFIT+t.SWAPS)/(SELECT AVERAGE FROM live1.daily_prices WHERE SYMBOL LIKE 'USDSGD' ORDER BY TIME DESC LIMIT 1) ELSE 0 END),2) 
                    AS `FLOATING PROFIT`,
                
                g.`COUNTRY` , u.`GROUP`
                        FROM Live{Live}.mt4_trades as t, Live{Live}.mt4_users as u, Live5.group_table as g
                        WHERE (OPEN_TIME >= NOW() - INTERVAL 1 DAY or CLOSE_TIME >=  NOW() - INTERVAL 1 DAY)
                        AND CMD < 2 AND t.LOGIN = u.LOGIN AND u.LOGIN>9999 AND u.login=t.login AND
                        g.`group` = t.`group` AND g.book in ("A", "B", "HK", "MAM") {Added_condition}
                        GROUP BY LOGIN, SYMBOL
                        ) T LEFT JOIN Live{Live}.symbol_rebate as r ON r.symbol=T.symbol
                
                GROUP BY LOGIN ) X LEFT JOIN 
                (Select live, login, max(lots) as lots, datetime from aaron.large_volume_login WHERE live = {Live} and datetime >= NOW() - INTERVAL 1 DAY GROUP BY LIVE, LOGIN) as L 
                ON X.login = L.login  
                WHERE `TOTAL LOTS` >= 50 """

    raw_sql_statement = raw_sql_statement.replace("\t", " ").replace("\n", "")
    # The string name of the live server.
    live_str = ["1", "2", "5"]
    sql_statement = " UNION ".join([raw_sql_statement.format(Live=l, Added_condition=" ") for l in live_str])
    sql_statement += " UNION " + raw_sql_statement.format(Live=3, Added_condition=" AND u.LOGIN NOT IN (SELECT LOGIN FROM live3.`cambodia_exclude` )")


    sql_statement += " ORDER BY `TOTAL LOTS` DESC "
    #print(sql_statement)

    res = Query_SQL_db_engine(text(sql_statement))
    df = pd.DataFrame(res)

    # Just in case there are no open trades. 
    if len(df) <= 0:
        return  json.dumps([{"Comment":"No trades."}])


    # Want to find out which level of "alert" it belongs to now.
    df["NEXT LEVEL"] = df["TOTAL LOTS"].apply(lambda x: min([a for a in alert_levels if a>=x ]))

    # Those that we need to send alerts for.
    to_alert = df[df["NEXT LEVEL"] > df["RECORDED LOTS"]]
    if len(to_alert) > 0:

        to_alert["DATETIME"] = "NOW()"  # Force it to be the datetime that MYSQL will insert.
        # Write to SQL first. After this, the LOGIN will change to LOGIN URL
        to_sql_values = ["({})".format(",".join(["{}".format(x) for x in l])) for l in to_alert[["LIVE", "LOGIN", "NEXT LEVEL", "DATETIME"]].values.tolist()]

        sql_insert = """INSERT INTO  aaron.`Large_Volume_Login` (`Live`, `Login`, `Lots`,  `datetime`) VALUES
           {} ON DUPLICATE KEY UPDATE `lots`=VALUES(`lots`) """.format(" , ".join(to_sql_values))
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")
        #print(sql_insert)

        #
        db.engine.execute(text(sql_insert))  # Insert into DB

        to_alert["LOGIN"] = to_alert.apply(lambda x: live_login_analysis_url_External(Live=x["LIVE"], Login=x["LOGIN"]), axis=1)

        telegram_string = "<b>[Alert]</b> - Large Volume Client Alert\n\n"
        telegram_string += "There are clients trading large volume in the past 24 hours.\n\n"
        telegram_string += "<b>Live | Login | Lots | C PnL | F.PnL | Rebate | Group</b>\n"
        telegram_string += "\n".join([" | ".join(["{}".format(x) for x in l]) for l in to_alert[["LIVE", "LOGIN", "TOTAL LOTS", "CLOSED PROFIT", "FLOATING PROFIT", "REBATE", "COUNTRY"]].values.tolist()])
        telegram_string += "\n\nDetails are on Client side."
        print(telegram_string)

        # async_Post_To_Telegram(TELE_ID_MONITOR, telegram_string, TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)

        async_Post_To_Telegram(BGI_MONITOR_TELEGRAM_TOKEN, telegram_string, \
                               [TELEGRAM_ALERT_GROUP_CHAT], Parse_mode=telegram.ParseMode.HTML)


        # If we wanna send out the alert as email as well.
        # email alert flag set in global file, in risk_tool_config
        email_flag, email_recipients = email_flag_fun("large_volume_alert")

        if email_flag_fun("large_volume_alert"):
            to_alert["CLOSED PROFIT"] =  to_alert["CLOSED PROFIT"].apply(profit_red_green)
            to_alert["FLOATING PROFIT"] = to_alert["FLOATING PROFIT"].apply(profit_red_green)
            data = to_alert[["LIVE", "LOGIN", "TOTAL LOTS", "FLOATING LOTS", "CLOSED PROFIT", "FLOATING PROFIT", "REBATE", "GROUP", "COUNTRY"]].to_dict("records")
            html_table = Array_To_HTML_Table( list(data[0].keys()), [list(data[i].values()) for i in range(len(data))], [])
            email_str = "Hi,<br><br>Kindly find clients that has large volume trades in the table below.<br>Details are on client side.<br><br>"
            email_str += html_table
            email_str += "This Email was generated at: SGT {}.<br><br>Thanks,<br>BGI Risk Team".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            async_send_email(To_recipients=email_recipients, cc_recipients=[], Subject="Large Volume Traders",
                             HTML_Text=Email_Header + email_str + Email_Footer, Attachment_Name=[])

        # email_html_body += "This Email was generated at: SGT {}.<br><br>Thanks,<br>Aaron".format(
        #datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        #flash("Live: {live}, Login: {login} Symbol: {Symbol} has been added to aaron.`client_zero_position`.".format(live=Live, login=Login, Symbol=Symbol))
        #print(to_sql_values)

    # Want to add the URL for Logins
    df["LOGIN"] = df.apply(lambda x: live_login_analysis_url(Live=x["LIVE"], Login=x["LOGIN"]), axis=1)
    # To add color when printed.
    df["CLOSED PROFIT"] = df["CLOSED PROFIT"].apply(lambda x: profit_red_green(x))
    df["FLOATING PROFIT"] = df["FLOATING PROFIT"].apply(lambda x: profit_red_green(x))

    df = df[["LOGIN", "LIVE", "TOTAL LOTS", "OPENED LOTS", "CLOSED LOTS",
         "FLOATING LOTS", "FLOATING PROFIT", "CLOSED PROFIT", "REBATE", "GROUP", "NEXT LEVEL"]]

    if update_tool_time == 1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="Large Lots Login")

    return json.dumps(df.to_dict("r"))

