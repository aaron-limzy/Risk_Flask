from app.extensions import  excel
from flask import render_template, flash, redirect, url_for, request, send_from_directory, jsonify, g, Markup, Blueprint, abort, current_app, session
from werkzeug.utils import secure_filename
from werkzeug.urls import url_parse

from app.forms import SymbolSwap, SymbolTotal, SymbolTotalTest, AddOffSet, MT5_Modify_Trades_Form, File_Form
from app.forms import LoginForm, CreateUserForm, noTrade_ChangeGroup_Form,equity_Protect_Cut,Live_Group, risk_AutoCut_Exclude


# Import function to call in case the page dosn't run.
from app.Plotly.routes import save_BGI_float_Ajax
from app.Scrape_Futures import *


from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField, HiddenField, FloatField, FormField
from flask_table import create_table, Col

import pyexcel
import urllib3
import decimal
import datetime

import os
import pandas as pd
import numpy as np
import requests
import json
import math
import asyncio
import psutil   # Want to get Computer CPU Details/Usage/Memory


from flask_login import current_user, login_user, logout_user, login_required
from app.models import User, load_user, flask_users
from sqlalchemy import text
from flask_sqlalchemy import SQLAlchemy
from Aaron_Lib import *

from aiopyfix.client_example import CFH_Position_n_Info

from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from requests import Session
from zeep.transports import Transport

from zeep import Client

import logging
#logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.client').setLevel(logging.DEBUG)
# logging.getLogger('suds.transport').setLevel(logging.DEBUG)
# logging.getLogger('suds.xsd.schema').setLevel(logging.DEBUG)
# logging.getLogger('suds.wsdl').setLevel(logging.DEBUG)


import plotly
import plotly.graph_objs as go
import plotly.express as px

import pandas as pd
import numpy as np
import json

from .decorators import async_fun
from io import StringIO


#simple_page = Blueprint('simple_page', __name__, template_folder='templates')


TIME_UPDATE_SLOW_MIN = 10




TELE_ID_MTLP_MISMATCH = "736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0"		# For Mismatch and LP Margin
TELE_ID_USDTWF_MISMATCH = "776609726:AAHVrhEffiJ4yWTn1nw0ZBcYttkyY0tuN0s"        # For USDTWF
TELE_CLIENT_ID = ["486797751"]        # Aaron's Telegram ID.

LP_MARGIN_ALERT_LEVEL = 20            # How much away from MC do we start making noise.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # To Display the warnings.


if get_machine_ip_address() == '192.168.64.73': #Only On Server computer
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com", "Risk@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com", "risk@blackwellglobal.com", "cs@bgifx.com"]
    print("On Server 64.73")
else:
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com"]
    print("On Aaron's Computer")

EMAIL_AARON =  ["aaron.lim@blackwellglobal.com"]     # For test Groups.


db = SQLAlchemy()  # <--- The db object belonging to the blueprint

#main_app = Blueprint('main_app', "main")

main_app = Blueprint('main_app', __name__)


@main_app.route('/')
@main_app.route('/index')        # Indexpage. To show the generic page.
@login_required
def index():
    # return render_template("index.html", header="index page", description="Hello")
    return render_template("index.html", header="Risk Tool.", description="Welcome {}".format(current_user.id))

@main_app.route('/login', methods=['GET', 'POST'])       # Login Page. If user is login-ed, will re-direct to index.
def login():

    if current_user.is_authenticated:
        return redirect(url_for('main_app.index'))
    # else:
        # print("User is not authenticated. ")

    form = LoginForm()
    if form.validate_on_submit():
        user = load_user(username=form.username.data.lower())
        if user is None or not user.check_password(form.password.data): # If Login error
            flash('Invalid username or password')
            return redirect(url_for('main_app.login'))
        else:
            flash('Login successful')
            login_user(user, remember=form.remember_me.data)

            next_page = request.args.get('next')
            if not next_page or url_parse(next_page).netloc != '':
                next_page = url_for('main_app.index')
            return redirect(next_page)

    return render_template('General_Form.html', title='Sign In',header="Sign In", form=form)


@main_app.route('/admin/create_user', methods=['GET', 'POST'])       # Login Page. If user is login-ed, will re-direct to index.
@login_required
def Create_User():

    form = CreateUserForm()
    if form.validate_on_submit():

        username = form.username.data.lower()
        email = form.email.data
        password = form.password.data
        admin_rights = 0

        user = flask_users.query.filter_by(username=username).first()
        if user is not None:
            flash("User {} is taken. Kindly choose another username.".format(username))  # Put a message out that there is some error.
        else:
            sql_insert = "INSERT INTO  aaron.`flask_login` (`username`, `email`, `password_hash`, `admin_rights`) VALUES" \
                " ('{}','{}','{}','{}')".format(username, email, User.hash_password(password=password), admin_rights)
            print(sql_insert)
            raw_insert_result = db.engine.execute(sql_insert)
            flash("User {} has been created.".format(username))  # Put a message out that there is some error.

    return render_template('General_Form.html', title='Create User',header="Create User", form=form)


@main_app.route('/logout', methods=['GET', 'POST'])  # Will logout the user.
@login_required
def logout():
    logout_user()
    return redirect(url_for('main_app.login'))


@main_app.route('/Dividend')
def Dividend():
    return render_template("base.html")


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

    return 'color: %s' % color



@main_app.route('/add_offset', methods=['GET', 'POST'])      # Want to add an offset to the ABook page.
@login_required
def add_off_set():
    form = AddOffSet()
    if request.method == 'POST' and form.validate_on_submit():
        symbol = form.Symbol.data       # Get the Data.
        offset = form.Offset.data
        ticket = form.Ticket.data
        lp = form.LP.data
        comment = form.Comment.data
        sql_insert = "INSERT INTO  test.`offset_live_trades` (`symbol`, `ticket`, `lots`, `Comment`, `datetime`, `lp`) VALUES" \
            " ('{}','{}','{}','{}',NOW(),'{}' )".format(symbol, ticket, offset, comment, lp)
        # print(sql_insert)
        db.engine.execute(sql_insert)   # Insert into DB
        flash("{symbol} {offset} updated in A Book offset.".format(symbol=symbol, offset=offset))

    raw_result = db.engine.execute("SELECT SYMBOL, SUM(LOTS) as 'BGI Lots' FROM test.`offset_live_trades` GROUP BY SYMBOL ORDER BY `BGI Lots` DESC")
    result_data = raw_result.fetchall()
    result_col = raw_result.keys()
    collate = [dict(zip(result_col, a)) for a in result_data]
    table = create_table_fun(collate)
    title = "Add offset"
    header="Adding A-Book Offset"
    description = "Adding to the off set table, to manage our position for A book end for tally sake."

    return render_template("General_Form.html", form=form, table=table, title=title, header=header,description=description)



# Want to change user group should they have no trades.
# ie: From B to A or A to B.
@main_app.route('/noopentrades_changegroup', methods=['GET', 'POST'])
@login_required
def noopentrades_changegroup():
    # TODO: Need to check insert return.


    form = noTrade_ChangeGroup_Form()

    title = "Change Group[No Open Trades]."
    header = "Change Group[No Open Trades]."
    description = "Running only on Live 1 and Live 3.<br>Will change the client's group based on data from SQL table: test.changed_group_opencheck<br>When CHANGED = 0."

    if request.method == 'POST' and form.validate_on_submit():
        live = form.Live.data       # Get the Data.
        login = form.Login.data
        current_group = form.Current_Group.data
        new_group = form.New_Group.data
        sql_insert = "INSERT INTO  test.`changed_group_opencheck` (`Live`, `login`, `current_group`, `New_Group`, `Changed`, `Time_Changed`) VALUES" \
            " ({live},{login},'{current_group}','{new_group}',{changed},now() )".format(live=live,login=login, current_group=current_group,new_group=new_group,changed=0)
        print(sql_insert)
        db.engine.execute(sql_insert)

    # elif request.method == 'POST' and form.validate_on_submit() == False:
    #  flash('Invalid Form Entry')


    return render_template("Change_USer_Group.html", form=form,title=title, header=header, description=Markup(description))


@main_app.route('/noopentrades_changegroup_ajax', methods=['GET', 'POST'])
@login_required
def noopentrades_changegroup_ajax(update_tool_time=1):
    # TODO: Check if user exist first.

    live_to_run = [1,2, 3, 5]  # Only want to run this on Live 1 and 3.

    # Raw SQL Statement. Will have to use .format(live=1) for example.
    raw_sql_statement = """SELECT mt4_users.LOGIN, X.LIVE, X.CURRENT_GROUP as `CURRENT_GROUP[CHECK]`, X.NEW_GROUP, mt4_users.`GROUP` as USER_CURRENT_GROUP,
            CASE WHEN mt4_users.`GROUP` = X.CURRENT_GROUP THEN 'Yes' ELSE 'No' END as CURRENT_GROUP_TALLY, 
            CASE WHEN X.NEW_GROUP IN (SELECT `GROUP` FROM Live{live}.mt4_groups WHERE `GROUP` LIKE X.NEW_GROUP) THEN 'Yes' ELSE 'No' END as NEW_GROUP_FOUND, 
            COALESCE((SELECT count(*) FROM live{live}.mt4_trades WHERE mt4_trades.LOGIN = X.LOGIN AND CLOSE_TIME = "1970-01-01 00:00:00" GROUP BY mt4_trades.LOGIN),0) as OPEN_TRADE_COUNT
            FROM Live{live}.mt4_users,(SELECT LIVE,LOGIN,CURRENT_GROUP,NEW_GROUP FROM test.changed_group_opencheck WHERE LIVE = '{live}' and `CHANGED` = 0 ) X
            WHERE mt4_users.`ENABLE` = 1 and mt4_users.LOGIN = X.LOGIN """

    raw_sql_statement = raw_sql_statement.replace("\t", " ").replace("\n", " ")
    # construct the SQL Statement
    sql_query_statement = " UNION ".join([raw_sql_statement.format(live=l) for l in live_to_run])
    sql_result = Query_SQL_db_engine(sql_query_statement)  # Query SQL

    return_val = {"All": [{"Comment":"Login awaiting change: 0", "Last Query time": "{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}]}     # Initialise.

    C_Return = dict()   # The returns from C++
    C_Return[0] = "User Changed"
    C_Return[-1] = "C++ ERROR: No Connection"
    C_Return[-2] = "C++ ERROR: New Group Not Found"
    C_Return[-3] = "C++ ERROR: User Current Group mismatched"
    C_Return[-4] = "C++ ERROR: Unknown Error"
    C_Return[-5] = "C++ ERROR: Open Trades"
    C_Return[-6] = "C++ ERROR: User Not Found!"
    C_Return[-10] = "C++ ERROR: Param Error"


    # Need to update Run time on SQL Update table.
    if update_tool_time == 1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="ChangeGroup_NoOpenTrades")

    if len(sql_result) == 0:    # If there are nothing to be changed.
        return_val = {"All": [{"Comment":"Login awaiting change: 0", "Last Query time": "{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}]}
        return json.dumps(return_val)

    success_result = []
    success_listdict = []
    for i, d in enumerate(sql_result):
        comment =""
        error_flag = 0

        if not ("CURRENT_GROUP_TALLY" in d and d["CURRENT_GROUP_TALLY"].find("Yes") >=0):
            comment += "Current group doesn't match. "
            error_flag += 1
        if not ("NEW_GROUP_FOUND" in d and d["NEW_GROUP_FOUND"].find("Yes") >=0):
            comment += "New group not found. "
            error_flag += 1
        if "OPEN_TRADE_COUNT" in d and d["OPEN_TRADE_COUNT"] == 0:
            if error_flag == 0: # No obvious error from the SQL Return Code.

                server = d["LIVE"] if "LIVE" in d else False
                login = d["LOGIN"] if "LOGIN" in d else False
                previous_group = d["USER_CURRENT_GROUP"] if "USER_CURRENT_GROUP" in d else False
                new_group = d["NEW_GROUP"] if "NEW_GROUP" in d else False
                if all([server,login,previous_group, new_group]):     # Need to run C++ Should there be anything to change.
                    # # (C_Return_Val, output, err)
                    # c_run_return= [0,0,0]   # Artificial Results.
                    c_run_return = Run_C_Prog("app" + url_for('static', filename='Exec/Change_User.exe') + " {server} {login} {previous_group} {new_group}".format(
                        server=server,login=login,previous_group=previous_group,new_group=new_group))


                    comment += C_Return[c_run_return[0]]
                    if c_run_return[0] == 0:    # If C++ Returns okay.
                        success_result.append((server, login, previous_group, new_group))
                        # Want to add into a list of dict. Want to change the _ of Keys to " " Spaces.
                        changed_user_buffer = dict(zip([k.replace("_", " ") for k in list(d.keys())], d.values()))
                        changed_user_buffer["UPDATED TIME"] = time_now()    # Want to add in the SGT that it was changed.
                        success_listdict.append(changed_user_buffer)
                else:
                    comment += "SQL Return Error. [LIVE, LOGIN, USER_CURRENT_GROUP, NEW_GROUP]"

        else:   # There are open trades. Cannot Change.
            comment += "Open trades found."
        sql_result[i]["Comment"] = comment if error_flag == 0 else "ERROR: {}".format(comment)

    if len(success_result) > 0:  # For those that are changed, We want to save it in SQL.
        # Want to get the WHERE statements for each.
        sql_where_statement = " OR ".join(["(LOGIN = {login} and LIVE = {server} and CURRENT_GROUP = '{previous_group}' and NEW_GROUP = '{new_group}') ".format(
            server=server, login=login,previous_group=previous_group,new_group=new_group) for (server, login, previous_group, new_group) in success_result])
        # print(sql_where_statement)

        # Construct the SQL Statement
        sql_update_statement = """UPDATE test.changed_group_opencheck set `CHANGED` = 1, TIME_CHANGED = now() where (	"""
        sql_update_statement += sql_where_statement
        sql_update_statement += """ ) and `CHANGED` = 0 """
        sql_update_statement = text(sql_update_statement)   # To SQL Friendly Text.
        print(sql_update_statement)
        raw_insert_result = db.engine.execute(sql_update_statement) # TO SQL


    val = [list(a.values()) for a in sql_result]
    key = list(sql_result[0].keys())
    key = [k.replace("_"," ") for k in key]     # Want to space things out instead of _
    return_val = {"All": [dict(zip(key,v)) for v in val], "Changed": success_listdict}




    # table = create_table_fun(sql_result)
    return json.dumps(return_val)


# Want to check and close off account/trades.
@main_app.route('/Risk_auto_cut', methods=['GET', 'POST'])
@login_required
def Risk_auto_cut():

    title = "Risk Auto Cut"
    header = "Risk Auto Cut"

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0


    description = Markup('Running only on <font color = "red"> Live 1 and Live 3</font>.<br>'   + \
                         "Will <b>close all client's position</b> and <b>change client to read-only</b>.<br>" + \
                         "Sql Table ( <font color = 'red'>aaron.risk_autocut_exclude</font>) for client excluded from the autocut.<br>" + \
                         "Sql Table ( <font color = 'red'>aaron.risk_autocut_include</font>) for client with special requests.<br><br>" + \
                         "<b>1)</b> For <font color = 'red'>%TW%</font> Clients : <br>EQUITY < CREDIT AND <br>((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND" + \
                                "<br>`ENABLE` = 1 AND ENABLE_READONLY = 0<br>and " + \
                                " LOGIN NOT IN ( <font color = 'red'>aaron.risk_autocut_exclude</font>)<br>and LOGIN IN ( <font color = 'red'>aaron.risk_autocut_include</font> where EQUITY_LIMIT <> 0 )<br><br>" + \
                         "<b>2)</b>For other clients, where GROUP` IN  <font color = 'red'>aaron.risk_autocut_group</font> and EQUITY < CREDIT and<br>" + \
                                " LOGIN NOT IN  (<font color = 'red'>aaron.risk_autocut_exclude</font>)<br>and LOGIN IN ( <font color = 'red'>aaron.risk_autocut_include</font> where EQUITY_LIMIT <> 0 )<br><br>" + \
                         "<b>3)</b> For Login in <font color = 'red'>aaron.Risk_autocut_exclude</font> and <font color = 'red'>Credit_limit != 0</font> and <br>" +\
                                " LOGIN NOT IN ( <font color = 'red'>aaron.risk_autocut_exclude</font>)<br>and LOGIN IN ( <font color = 'red'>aaron.risk_autocut_include</font> where EQUITY_LIMIT <> 0 )<br><br>" + \
                         "<b>4)</b> Tool will not cut for <font color = 'red'>Equity > Credit</font> . For such, kindly look at Equity Protect.<br><br>")

        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/Charts.jpg', Table_name="Risk Auto Cut", \
                           title=title, ajax_url=url_for('main_app.risk_auto_cut_ajax', _external=True), header=header, setinterval=10,
                           description=description, replace_words=Markup(["Today"]))


@main_app.route('/risk_auto_cut_ajax', methods=['GET', 'POST'])
@login_required
def risk_auto_cut_ajax(update_tool_time=1):
    # TODO: Check if user exist first.

    #print("Risk Auto Cut Ajax")
    # Using External Table
    # aaron.risk_autocut_exclude
    # aaron.risk_autocut_group

    Live_server = [1,2,3,5]

    # For %TW% Clients where EQUITY < CREDIT AND ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0
    # For other clients, where GROUP` IN  aaron.risk_autocut_group and EQUITY < CREDIT
    # For Login in aaron.Risk_autocut and Credit_limit != 0

    # To check the Lucky Draw Login. All TW clients for login not in aaron.risk_autocut_exclude
    # Also done a check to cause hedging clients to SO
    raw_sql_statement = """SELECT LOGIN, '3' as LIVE, mt4_users.`GROUP`, ROUND(EQUITY, 2) as EQUITY, ROUND(CREDIT, 2) as CREDIT
            FROM live3.mt4_users WHERE `GROUP` LIKE '%TW%' AND EQUITY < CREDIT AND 
        ((CREDIT = 0 AND BALANCE > 0) OR CREDIT > 0) AND `ENABLE` = 1 AND ENABLE_READONLY = 0 and
        LOGIN not in (select login from aaron.risk_autocut_exclude where LIVE = 3) and 
        LOGIN not in (select login from aaron.risk_autocut_include where LIVE = 3 and EQUITY_LIMIT <> 0)"""

    raw_sql_statement = raw_sql_statement.replace("\t", " ").replace("\n", " ")
    sql_result1 = Query_SQL_db_engine(text(raw_sql_statement))  # Query SQL


    # For the client's whose groups are in aaron.risk_autocut_group, and login not in aaron.risk_autocut_exclude
    raw_sql_statement = """SELECT DISTINCT mt4_trades.LOGIN,'{Live}' AS LIVE,  mt4_users. `GROUP`, ROUND(EQUITY, 2) as EQUITY, ROUND(CREDIT, 2) as CREDIT
    FROM live{Live}.mt4_trades, live{Live}.mt4_users WHERE
    mt4_trades.LOGIN = mt4_users.LOGIN AND 
        ( mt4_users.`GROUP` IN (SELECT `GROUP` FROM aaron.risk_autocut_group WHERE LIVE = '{Live}') 
                                OR
        mt4_users.LOGIN IN (SELECT LOGIN FROM aaron.risk_autocut_include WHERE LIVE = '{Live}' and EQUITY_LIMIT = 0)
        )    AND 
    CLOSE_TIME = '1970-01-01 00:00:00' AND 
    CMD < 6 AND 
    EQUITY < CREDIT AND 
    mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM aaron.risk_autocut_exclude WHERE `LIVE` = '{Live}') AND
    mt4_trades.LOGIN NOT IN (SELECT LOGIN FROM aaron.risk_autocut_include WHERE `LIVE` = '{Live}' and EQUITY_LIMIT <> 0)"""

    raw_sql_statement = " UNION ".join([raw_sql_statement.format(Live=n) for n in Live_server])  # construct the SQL Statment
    raw_sql_statement = raw_sql_statement.replace("\t", " ").replace("\n", " ")
    sql_result2 = Query_SQL_db_engine(text(raw_sql_statement))  # Query SQL



    # For the client's who are in aaron.risk_autocut_include and Credit_limit != 0
    raw_sql_statement = """SELECT DISTINCT T.LOGIN, {Live} AS LIVE,  U.`GROUP`, ROUND(EQUITY, 2) as EQUITY, ROUND(CREDIT, 2) as CREDIT, R.EQUITY_LIMIT as EQUITY_LIMIT
    FROM live{Live}.mt4_users as U, aaron.risk_autocut_include as R, live{Live}.mt4_trades as T
    WHERE R.LIVE = {Live} and R.EQUITY_LIMIT != 0 and
    R.LOGIN = U.LOGIN and U.EQUITY < R.EQUITY_LIMIT and
    U.LOGIN = T.LOGIN and T.CLOSE_TIME = '1970-01-01 00:00:00' AND
    U.LOGIN NOT IN (SELECT LOGIN FROM aaron.risk_autocut_exclude WHERE `LIVE` = '{Live}')"""

    raw_sql_statement = " UNION ".join([raw_sql_statement.format(Live=n) for n in Live_server])  # construct the SQL Statment
    raw_sql_statement = raw_sql_statement.replace("\t", " ").replace("\n", " ")
    sql_result3 = Query_SQL_db_engine(text(raw_sql_statement))  # Query SQL

    #sql_result3 = []

    total_result = dict()
    sql_results = [sql_result1, sql_result2, sql_result3]
    for s in sql_results:   # We want only unique values.
        for ss in s:
            live = ss["LIVE"] if "LIVE" in ss else None
            login = ss["LOGIN"] if "LOGIN" in ss else None
            if live != None and login != None and (live,login) not in total_result:
                total_result[(live,login)] = ss

    C_Return = dict()  # The returns from C++
    C_Return[0] = "User Changed to Read-Only, position forced closed";
    C_Return[-1] = "C++ ERROR: Params Wrong";
    C_Return[1] = "C++ ERROR: Login Param Error";
    C_Return[2] = "C++ ERROR: Server Param Error";
    C_Return[3] = "C++ ERROR: Login Number Error on MT4";
    C_Return[4] = "C++ ERROR: Equity Above Credit";
    C_Return[6] = "C++ ERROR: MT4 Update Error!";
    C_Return[7] = "C++ ERROR: MT4 Connection Error";
    C_Return[-100] = "User changed to Read-only, but SQL ERROR. ";

    To_SQL = []     # To save onto SQL

    for k,d in total_result.items():
        live = d['LIVE'] if "LIVE" in d else None
        login = d['LOGIN'] if "LOGIN" in d else None
        equity_limit = d['EQUITY_LIMIT'] if "EQUITY_LIMIT" in d else 0
        if equity_limit == 0:   # Adding this in to beautify the Table.
            total_result[k]["EQUITY_LIMIT"] = "-"
        if not None in [live, login]:   # If both are not None.

            # # print("Live = {}, Login = {}, equity_limit = {}".format(live, login, equity_limit))


            c_run_return = Run_C_Prog("app" + url_for('static', filename='Exec/Risk_Auto_Cut.exe') + " {live} {login} {equity_limit}".format( \
            live=live, login=login,equity_limit=equity_limit))
            #print("c_run_return = {}".format(c_run_return))
            # c_run_return = 0

            if c_run_return[0] == 0:  # Need to save things into SQL as well.
                To_SQL.append(d)
            # elif c_run_return[0] not in C_Return:
            #     print(c_run_return)

            total_result[k]["RESULT"] = C_Return[c_run_return[0]] if c_run_return[0] in C_Return else "Unknown Error"

    return_val = dict()  # Return value to be used
    if len(total_result) > 0:   # Want to send out an email should any changes have been made.

        if len(To_SQL) > 0: # There might not be anything here due to an error in C exe
            raw_insert_sql = " ({live}, {login}, {equity}, {credit}, '{group}', now()) "    # Raw template for insert.
            sql_insert_w_values = ",".join([raw_insert_sql.format(live=d["LIVE"], login=d["LOGIN"], equity=d["EQUITY"], credit=d["CREDIT"], group=d["GROUP"]) for d in To_SQL]) # SQL insert with the values.
            sql_insert = "INSERT INTO  aaron.`risk_autocut_results` (LIVE, LOGIN, EQUITY, CREDIT, `GROUP`, DATE_TIME) VALUES {}".format(sql_insert_w_values)   # Add it to the header.
            sql_insert += " ON DUPLICATE KEY UPDATE `EQUITY`=VALUES(`EQUITY`), `CREDIT`=VALUES(`CREDIT`), `GROUP`=VALUES(`GROUP`)  "


            #print("SQL Statement: {}".format(sql_insert))
            raw_insert_result = db.engine.execute(sql_insert)   # Insert into SQL

        #print("total_result: {}".format(total_result))

        total_result_value = list(total_result.values())
        # print(total_result_value)
        table_data_html =  Array_To_HTML_Table(list(total_result_value[0].keys()),[list(d.values()) for d in total_result_value])

        # Want to set to test, if it's just test accounts.
        email_list = EMAIL_AARON if all([d["GROUP"].lower().find("test") >= 0 for d in To_SQL]) else EMAIL_LIST_BGI

        async_send_email(To_recipients=email_list, cc_recipients=[],
                     Subject="AutoCut: Equity Below Credit.",
                     HTML_Text="{Email_Header}Hi,<br><br>The following client/s have had their position closed, and has been changed to read-only, as their equity was below credit. \
                                <br><br> {table_data_html} This is done to prevent client from trading on credit. \
                               <br><br>This Email was generated at: {datetime_now} (SGT)<br><br>Thanks,<br>Aaron{Email_Footer}".format(
                         Email_Header = Email_Header, table_data_html = table_data_html, datetime_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         Email_Footer=Email_Footer), Attachment_Name=[])



        return_val = list(total_result.values())

    else:   # If there are nothing to change. We want to show the time
        return_val = [{"RESULT": "No clients to be changed. Time: {}".format(time_now())}]

    # # Need to update Run time on SQL Update table.
    if update_tool_time ==1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="Risk_Auto_Cut")


    return json.dumps(return_val)












# Want to check and close off account/trades.
@main_app.route('/USOil_Price_Alerts', methods=['GET', 'POST'])
@login_required
def USOil_Ticks():

    title = "USOil Monitor"
    header = "USOil Monitor"


    description = Markup('Check ticks from 64.56 db.<br>Will need to check if USOil falls below 5.')

        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/Oil_Rig_2.jpg', Table_name="USOil Ticks", \
                           title=title, ajax_url=url_for('main_app.USOil_Ticks_ajax', _external=True), header=header, setinterval=20,
                           description=description, replace_words=Markup(["Today"]))


@main_app.route('/USOil_Price_Ticks_ajax', methods=['GET', 'POST'])
@login_required
def USOil_Ticks_ajax(update_tool_time=1):


    # No need to run if it ran before.
    if all(["USOil_{}_Alert".format(u) in session for u in [5,0]]):
        return_val = [{"RESULT": "USOil_0_Alert ran at {}, USOil_5_Alert ran at {}".format(session['USOil_0_Alert'], session['USOil_5_Alert'])}]
        return json.dumps(return_val)

    update_date_time = "No return from SQL Ticks 64.56"
    usoil_mid_price = 10    # We don't want to accidentally trigger this...
    # Query tick DB for USOil Ticks
    sql_return = Query_SQL_Host("SELECT LOCAL_DATE_TIME, bid FROM bgi_live3.`.usoil.d_ticks` ORDER BY DATE_TIME DESC limit 1", "192.168.64.56", 'risk', 'Riskrisk321', 'bgi_live1')
    #print(sql_return)

    if len(sql_return) == 2 and len(sql_return[0]) > 0 and len(sql_return[0][0]) > 0:
        usoil_res = sql_return[0][0]
    if len(usoil_res) == 2:
        update_date_time, usoil_mid_price = usoil_res

    #default return val.
    return_val = [
        {"RESULT": "USOil Price ({}). SQL Update Time: {}. Time Now: {}".format(usoil_mid_price, update_date_time, time_now())}]



    # Run the tests.
    # USOil_Price_Alert_Array = {5: {'path':'Edit_Symbol_Settings_TEST.exe Check', 'cwd':".\\app" + url_for('static', filename='Exec/USOil_Symbol_Closed_Only')},
    #                            0.01 : {'path':'Close_USOil_Trade_Test.exe', 'cwd':".\\app" + url_for('static', filename='Exec/USOil_Close_Trades')}} # The 2 values that we need to care about.


    # Run the real prog
    USOil_Price_Alert_Array = {5: {'path':'Edit_Symbol_Setting.exe Check', 'cwd':".\\app" + url_for('static', filename='Exec/USOil_Symbol_Closed_Only')},
                               0.01 : {'path':'Close_USOil_Trade_0.01.exe', 'cwd':".\\app" + url_for('static', filename='Exec/USOil_Close_Trades')}} # The 2 values that we need to care about.

    #usoil_mid_price = 0
    for USOil_Price_Alert_Actual in USOil_Price_Alert_Array:


        USOil_Price_Alert = round(USOil_Price_Alert_Actual)   # The alert Price. Want to do a rounding since 0.01 is hard to match



        if usoil_mid_price <= USOil_Price_Alert_Actual:     # If the price fell below that.
            print("USOil price fell below {}...".format(USOil_Price_Alert))
            if "USOil_{}_Alert".format(USOil_Price_Alert) not in session:  # save in session that we have already sent out an email.
                print("Need to react to this..")

                # Want to check from SQL if the price has been activated.
                sql_query = text("select result from aaron.aaron_misc_data where item = 'USOil_Price_{}_Activated'".format(USOil_Price_Alert))

                raw_result = db.engine.execute(sql_query)
                result_data = raw_result.fetchall()
                print(result_data)
                if len(result_data) > 0 and len(result_data[0]) > 0:
                    tool_ran_result = result_data[0][0]
                    session['USOil_{}_Alert'.format(USOil_Price_Alert)] = Get_time_String()
                    if tool_ran_result != '0':    # tool has been ran..
                        print("SQL: Ran at {}".format(tool_ran_result))
                        pass
                    else:

                        #print("Running tool...")
                        async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, "USOil has dropped below ${}".format(USOil_Price_Alert), TELE_CLIENT_ID)
                        # Will run C here as well..

                        c_run_return = Run_C_Prog(Path=USOil_Price_Alert_Array[USOil_Price_Alert_Actual]['path'],
                                                  cwd=USOil_Price_Alert_Array[USOil_Price_Alert_Actual]['cwd'])

                        # #c_run_return = Run_C_Prog("app" + url_for('static', filename='Exec/USOil_Symbol_Closed_Only/Close_USOil_Trade_0.01.exe') + " Edit")
                        #print("c_run_return = {}".format(c_run_return))
                        #c_run_return = 0

                        # Catch C return.
                        (C_Return_Val, output, err) = c_run_return
                        output = output.decode()
                        output = output.replace("\r\n", "<br>") # Need to replace the C string of \n to HTML <br>

                        #
                        async_send_email(To_recipients=EMAIL_LIST_BGI, cc_recipients=[],
                                     Subject="USOil Below {} Dollars.".format(USOil_Price_Alert),
                                     HTML_Text="""{Email_Header}Hi,<br><br>USOil Price is at {usoil_mid_price}, and it has dropped below {USOil_Price_Alert} USD. <br> 
                                                 The following is the C output. <br><br>{c_output}<br><br>
                                               <br><br>This Email was generated at: {datetime_now} (SGT)<br><br>Thanks,<br>Aaron{Email_Footer}""".format(
                                         Email_Header = Email_Header, USOil_Price_Alert = USOil_Price_Alert, usoil_mid_price = usoil_mid_price, c_output=output, datetime_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                         Email_Footer=Email_Footer), Attachment_Name=[])

                        # Update SQL
                        sql_insert = """UPDATE aaron.aaron_misc_data SET result= '{}' where item = 'USOil_Price_{}_Activated'""".format(Get_time_String(),USOil_Price_Alert)
                        sql_insert = sql_insert.replace("\t", "").replace("\n", "")
                        #
                        # print(sql_insert)
                        db.engine.execute(text(sql_insert))  # Insert into DB


                        print("Tool ran: {tool_ran_result}".format(tool_ran_result=tool_ran_result))




    # # Need to update Run time on SQL Update table.
    if update_tool_time ==1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="USOil_Price_alert")


    return json.dumps(return_val)

















# Want to insert into table.
# From Flask.
@main_app.route('/Risk_Autocut_include', methods=['GET', 'POST'])
@login_required
def Include_Risk_Autocut():
    title = Markup("Include Risk Auto Cut")
    header = title
    description = Markup(
        """<b>To Include into the running tool of Risk Auto Cut</b>
        <br>Will add account into aaron.risk_autocut_include.<br>
        To include client from being autocut.<br>
        If Equity_Limit = 0, will cut normally when Equity < credit""")

    form = equity_Protect_Cut()
    #print("Method: {}".format(request.method))
    #print("validate_on_submit: {}".format(form.validate_on_submit()))
    form.validate_on_submit()
    if request.method == 'POST' and form.validate_on_submit():
        Live = form.Live.data  # Get the Data.
        Login = form.Login.data
        Equity_Limit = form.Equity_Limit.data

        sql_insert = """INSERT INTO  aaron.`risk_autocut_Include` (`Live`, `Login`, `Equity_Limit`) VALUES
            ('{Live}','{Account}','{Equity}') ON DUPLICATE KEY UPDATE `Equity_Limit`=VALUES(`Equity_Limit`) """.format(Live=Live, Account=Login, Equity=Equity_Limit)
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")

        print(sql_insert)
        db.engine.execute(text(sql_insert))  # Insert into DB
        flash("Live: {live}, Login: {login} Equity limit: {equity_limit} has been added to aaron.`risk_autocut_Include`.".format(live=Live, login=Login, equity_limit=Equity_Limit))

    # TODO: Add Form to add login/Live/limit into the include table.
    return render_template("General_Form.html",
                           title=title, header=header,
                           form=form, description=description)



# Want to insert into table.
# From Flask.
@main_app.route('/Risk_Autocut_exclude', methods=['GET', 'POST'])
@login_required
def Exclude_Risk_Autocut():
    title = Markup("Exclude Risk Auto Cut")
    header = title
    description = Markup(
        """<b>To Exclude into the running tool of Risk Auto Cut</b>
        <br>Will add account into aaron.risk_autocut_Exclude.<br>
        To Exclude client from being autocut.""")

    form = risk_AutoCut_Exclude()
    #print("Method: {}".format(request.method))
    #print("validate_on_submit: {}".format(form.validate_on_submit()))
    form.validate_on_submit()
    if request.method == 'POST' and form.validate_on_submit():
        Live = form.Live.data  # Get the Data.
        Login = form.Login.data

        sql_insert = """INSERT INTO  aaron.`risk_autocut_exclude` (`Live`, `Login`) VALUES
            ('{Live}','{Account}') ON DUPLICATE KEY UPDATE Login=VALUES(Login)  """.format(Live=Live, Account=Login)
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")

        print(sql_insert)
        db.engine.execute(text(sql_insert))  # Insert into DB
        flash("Live: {live}, Login: {login} has been added to aaron.`risk_autocut_exclude`.".format(live=Live, login=Login))

    # TODO: Add Form to add login/Live/limit into the include table.
    return render_template("General_Form.html",
                           title=title, header=header,
                           form=form, description=description)





# Want to insert into table.
# From Flask.
@main_app.route('/Risk_Autocut_Include_Group', methods=['GET', 'POST'])
@login_required
def Include_Risk_Autocut_Group():
    title = Markup("Include<br>Client Group into<br>Risk Auto Cut")
    header = title
    description = Markup(
        "<b>To Include the Client Group into the running tool of Risk Auto Cut</b><br>")

    form = Live_Group()
    #print("Method: {}".format(request.method))
    #print("validate_on_submit: {}".format(form.validate_on_submit()))
    form.validate_on_submit()
    if request.method == 'POST' and form.validate_on_submit():
        Live = form.Live.data  # Get the Data.
        Client_Group = form.Client_Group.data

        sql_insert = """INSERT INTO  aaron.`risk_autocut_group` (`Live`, `GROUP`) VALUES
            ('{Live}','{Group}')""".format(Live=Live, Group=Client_Group)
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")

        print(sql_insert)
        db.engine.execute(text(sql_insert))  # Insert into DB
        flash("Live: {live}, Group: {Group} has been added to aaron.`risk_autocut_group`.".format(live=Live, Group=Client_Group))

    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("General_Form.html",
                           title=title, header=header,
                           form=form, description=description)


# Want to check and close off account/trades.
@main_app.route('/Equity_Protect', methods=['GET', 'POST'])
@login_required
def Equity_protect():

    title = "Equity Protect"
    header = "Equity Protect"
    description = Markup("Equity Protect Cut.<br>Will Cut position if Equity below a certain level.<br>" + \
                         "Need to look into the table aaron.risk_equity_protect_cut.")
    form = equity_Protect_Cut()
    if request.method == 'POST' and form.validate_on_submit():
        Live = form.Live.data       # Get the Data.
        Login = form.Login.data
        Equity_Limit = form.Equity_Limit.data


        sql_insert = """INSERT INTO  aaron.`risk_equity_protect_cut` (`Live`, `Account`, `Equity`) VALUES
            ('{Live}','{Account}','{Equity}') ON DUPLICATE KEY UPDATE Equity=VALUES(Equity)""".format(Live=Live, Account=Login, Equity=Equity_Limit)
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")

        print(sql_insert)
        db.engine.execute(text(sql_insert))   # Insert into DB
        flash("Live: {Live}, Account: {Account}, Equity: {Equity} updated in aaron.`risk_equity_protect_cut`.".format(Live=Live, Account=Login, Equity=Equity_Limit))


    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/Equity_cut.jpg', Table_name="Equity Protect Cut", \
                           title=title, ajax_url=url_for('main_app.Equity_protect_Cut_ajax',_external=True), header=header,
                           form=form,setinterval = 20,
                           description=description, replace_words=Markup(["Today"]))


# Cut position when equity falls below a certain level
@main_app.route('/Equity_protect_ajax', methods=['GET', 'POST'])
#@login_required
def Equity_protect_Cut_ajax(update_tool_time=1):

    # # #time.sleep(5)
    # # return_val = [{"Result": "No Client to change. {}".format(time_now())}]
    # return_val = [{"LIVE":1,"LOGIN":"2040😂","BALANCE":-120.18,"EQUITY":123.341,"GROUP":"0_Test_Risk","EQUITY_CUT":10000,"RUN_RESULTS":"ALL_DONE"}]
    # # # return json.dumps("[Hello")
    # return json.dumps(return_val)

    #TODO: Send to Risk only, for a test account.

    # TODO: Check if user exist first.
    raw_sql_statement = """SELECT risk_equity_protect_cut.LIVE,mt4_users.LOGIN,mt4_users.BALANCE,mt4_users.EQUITY,mt4_users.`GROUP`,risk_equity_protect_cut.Equity as EQUITY_CUT 
    FROM live1.mt4_users,aaron.risk_equity_protect_cut 
    WHERE mt4_users.LOGIN = risk_equity_protect_cut.Account AND risk_equity_protect_cut.Live = '1' AND 
    mt4_users.EQUITY < risk_equity_protect_cut.Equity AND mt4_users.BALANCE + mt4_users.CREDIT - mt4_users.EQUITY <> 0
    UNION
    SELECT risk_equity_protect_cut.LIVE,mt4_users.LOGIN,mt4_users.BALANCE,mt4_users.EQUITY,mt4_users.`GROUP`,risk_equity_protect_cut.Equity as EQUITY_CUT 
    FROM live2.mt4_users,aaron.risk_equity_protect_cut 
    WHERE mt4_users.LOGIN = risk_equity_protect_cut.Account AND risk_equity_protect_cut.Live = '2' AND mt4_users.EQUITY < risk_equity_protect_cut.Equity AND 
    mt4_users.BALANCE + mt4_users.CREDIT - mt4_users.EQUITY <> 0
    UNION
    SELECT risk_equity_protect_cut.LIVE,mt4_users.LOGIN,mt4_users.BALANCE,mt4_users.EQUITY,mt4_users.`GROUP`,risk_equity_protect_cut.Equity as EQUITY_CUT 
    FROM live3.mt4_users,aaron.risk_equity_protect_cut 
    WHERE mt4_users.LOGIN = risk_equity_protect_cut.Account AND risk_equity_protect_cut.Live = '3' AND mt4_users.EQUITY < risk_equity_protect_cut.Equity AND 
    mt4_users.BALANCE + mt4_users.CREDIT - mt4_users.EQUITY <> 0
    UNION
    SELECT risk_equity_protect_cut.LIVE,mt4_users.LOGIN,mt4_users.BALANCE,mt4_users.EQUITY,mt4_users.`GROUP`,risk_equity_protect_cut.Equity as EQUITY_CUT 
    FROM live5.mt4_users,aaron.risk_equity_protect_cut 
    WHERE mt4_users.LOGIN = risk_equity_protect_cut.Account AND risk_equity_protect_cut.Live = '5' AND mt4_users.EQUITY < risk_equity_protect_cut.Equity AND 
    mt4_users.BALANCE + mt4_users.CREDIT - mt4_users.EQUITY <> 0"""

    sql_result = Query_SQL_db_engine(text(raw_sql_statement))  # Query SQL
    if update_tool_time == 1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="Equity_protect")


    if len(sql_result) > 0:
        return_val = sql_result
    else:   # Nothing to do. Will return.
        return_val = [{"Result" : "No Client to change. {}".format(time_now())}]
        #return json.dumps("[Hello")
        return json.dumps(return_val)

    c_return = {}
    c_return[0] = "ALL_DONE"
    c_return[1] = "LOGIN_NUM_ERROR"
    c_return[2] = "SERVER_NUM_ERROR"
    c_return[3] = "LOGIN_NUM_ERROR"
    c_return[4] = "EQUITY_ABOVE_LIMIT"
    c_return[5] = "OPEN_POSITION"
    c_return[6] = "MT4_UPDATE_ERROR"
    c_return[7] = "MT4_CONNECTION_ERROR"
    c_return[8] = "EQUITY_WITHIN_EQUITY_LIMIT"
    c_return[9] = "SQL RETURN ERROR."  # Not in C return. Added my Python

    success_change = []
    failed_change = []

    for i in range(len(sql_result)):
        live = sql_result[i]["LIVE"] if "LIVE" in sql_result[i] else -1
        login = sql_result[i]["LOGIN"] if "LOGIN" in sql_result[i] else -1
        equity_cut = sql_result[i]["EQUITY_CUT"] if "EQUITY_CUT" in sql_result[i] else -1

        if not any([live==-1, login==-1, equity_cut==-1]):      # Need to ensure we have the correct input
            c_run_return = Run_C_Prog("app" + url_for('static', filename='Exec/Risk_Equity_Protect.exe') + " {live} {login} {equity_cut}".format( \
               live=live, login=login,equity_cut=equity_cut))
            sql_result[i]["RUN_RESULTS"] = c_return[c_run_return[0]] if c_run_return[0] in c_return else "Unknown error: {}".format(c_run_return)


            if c_run_return[0] == 0: # Successfully changed.
                success_change.append(sql_result[i])
            else:
                failed_change.append(sql_result[i])
            #c_run_return = [0,1]
        else:
            sql_result[i]["RUN_RESULTS"] = "SQL Return Error."
            failed_change.append(sql_result[i])

    if len(success_change) > 0:
        table_data_html = Array_To_HTML_Table(list(success_change[0].keys()),
                                              [list(d.values()) for d in success_change])
        async_send_email(To_recipients=EMAIL_LIST_BGI, cc_recipients=[],
                     Subject="Equity Protection cut.",
                     HTML_Text="{Email_Header}Hi,<br><br>The following client/s have had their position closed, and has been changed to read-only, as their equity was below limit.. \
                                <br><br> {table_data_html} This is done to protect client equity. \
                               <br><br>This Email was generated at: {datetime_now} (SGT)<br><br>Thanks,<br>Aaron{Email_Footer}".format(
                         Email_Header = Email_Header, table_data_html = table_data_html, datetime_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         Email_Footer=Email_Footer), Attachment_Name=[])

    if len(failed_change) > 0:
        table_data_html = Array_To_HTML_Table(list(failed_change[0].keys()),
                                              [list(d.values()) for d in failed_change])

        async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                         Subject="Error: Equity Protection cut.",
                         HTML_Text="{Email_Header}Hi,<br><br>The following client/s have equity below limit, but was unable to close due to errors. \
                                        <br><br> {table_data_html}\
                                       <br><br>This Email was generated at: {datetime_now} (SGT)<br><br>Thanks,<br>Aaron{Email_Footer}".format(
                             Email_Header=Email_Header, table_data_html=table_data_html,
                             datetime_now=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             Email_Footer=Email_Footer), Attachment_Name=[])


    return json.dumps(return_val)
    #return json.dumps("Hello World")



# Want to check and close off account/trades.
@main_app.route('/CFH_Live_Position', methods=['GET', 'POST'])
@login_required
def CFH_Soap_Position():

    title = "CFH_Live_Position"
    header = "CFH_Live_Position"
    description = Markup("Getting CFH Live Position from CFH BO via CFH's SOAP API.<br>Results will be inserted/updated to aaron.cfh_live_trades.<br>Update time in table is GMT.")

    return render_template("Standard_Single_Table.html", backgroud_Filename='css/notebook_pen.jpg', Table_name="CFH Live Position", \
                           title=title, ajax_url=url_for('main_app.CFH_Soap_Position_ajax'), header=header, setinterval=60*60*12,
                           description=description, replace_words=Markup(["Today"]))

@main_app.route('/CFH_Live_Position_ajax', methods=['GET', 'POST'])
@login_required
def CFH_Soap_Position_ajax(update_all=0):  # Optional Parameter, to update from the start should there be a need to.
    # TODO: Update Minitor Tools Table.

    wsdl_url = "https://ws.cfhclearing.com:8094/ClientUserDataAccess?wsdl"
    session_soap = Session()
    session_soap.auth = HTTPBasicAuth("BG_Michael", current_app.config["CFH_BO_PASSWORD"])
    client = Client(wsdl_url, transport=Transport(session=session_soap))

    database_name = "aaron"
    table_name = "CFH_Live_Trades"

    #TODO: See if we need to run this every day to update the trades?
    update_all = 1  # Hard code for now. Will update later

    # Want to get the Client number that BGI has with CFH.
    client_details = client.service.GetAccounts()
    client_num = client_details[0].AccountId if len(client_details) > 0 else -1

    # When the start dates are.  Want to backtrace 2 hours at all normal times. Want to backtrace till start (sept 1st) if needed.
    query_start_date = (datetime.datetime.now() - datetime.timedelta(hours=2)) if update_all==0 else datetime.date(2019, 9, 1)
    # query_end_date =  datetime.date(2019, 9, 1)
    query_end_date = datetime.date.today()  # Always will end at today, now.

    # Flags and lists to store the data.
    total_trades = []
    total_pages = 1
    page_counter = 0

    # To get trades.
    while total_pages != 0 or total_pages == (
            page_counter + 1):  # 27840 is the account number.
        loop_trades = client.service.GetTrades(client_num, query_start_date, query_end_date, 1000, page_counter)
        total_pages = loop_trades.TotalPages if "TotalPages" in loop_trades else 0
        page_counter = page_counter + 1
        if loop_trades.TradesList != None:  # Would be None if there are no more trades.
            for t in loop_trades.TradesList.TradeInfo:
                total_trades.append(t)

    return_val = [{"Results": "No Trades return "}]

    if len(total_trades) > 0:   # If there are no Trades.
        # To write the SQL statement for insert
        # .
        live_trades_sql_header = """INSERT INTO {database_name}.{table_name} (`Amount`,	`BoTradeId`,	`Cancelled`,	`ClientOrderId`,	`Closed`,
        `Commission`,	`CommissionCurrency`,	`ExecutionDate`,	`InstrumentId`,	`OrderId`,	`Price`,	`Side`,	`Track`,	`TradeDate`,
            `TradeSystemId`,	`TradeType`,	`TsTradeId`,	`ValueDate`,	`ExternalClientId`, `Updated_time`) VALUES """.format(
            database_name=database_name, table_name=table_name)

        live_trade_values = " , ".join([""" ('{Amount}',	'{BoTradeId}',	'{Cancelled}',	'{ClientOrderId}',	'{Closed}',	'{Commission}',
        '{CommissionCurrency}',	'{ExecutionDate}',	'{InstrumentId}',	'{OrderId}',	'{Price}',	'{Side}',	'{Track}',	'{TradeDate}',	'{TradeSystemId}',
        '{TradeType}',	'{TsTradeId}',	'{ValueDate}',	'{ExternalClientId}', DATE_SUB(now(),INTERVAL 8 HOUR)) """.format(Amount=t.Amount,
                                         BoTradeId=t.BoTradeId, Cancelled=t.Cancelled, ClientOrderId=t.ClientOrderId,
                                         Closed=t.Closed, Commission=t.Commission, CommissionCurrency=t.CommissionCurrency,
                                         ExecutionDate=t.ExecutionDate, InstrumentId=t.InstrumentId, OrderId=t.OrderId,
                                         Price=t.Price, Side=t.Side, Track=t.Track, TradeDate=t.TradeDate,
                                         TradeSystemId=t.TradeSystemId, TradeType=t.TradeType, TsTradeId=t.TsTradeId,
                                         ValueDate=t.ValueDate, ExternalClientId=t.ExternalClientId) for t in total_trades])


        live_trade_values = [""" ('{Amount}',	'{BoTradeId}',	'{Cancelled}',	'{ClientOrderId}',	'{Closed}',	'{Commission}',
        '{CommissionCurrency}',	'{ExecutionDate}',	'{InstrumentId}',	'{OrderId}',	'{Price}',	'{Side}',	'{Track}',	'{TradeDate}',	'{TradeSystemId}',
        '{TradeType}',	'{TsTradeId}',	'{ValueDate}',	'{ExternalClientId}', DATE_SUB(now(),INTERVAL 8 HOUR)) """.format(Amount=t.Amount,
                                         BoTradeId=t.BoTradeId, Cancelled=t.Cancelled, ClientOrderId=t.ClientOrderId,
                                         Closed=t.Closed, Commission=t.Commission, CommissionCurrency=t.CommissionCurrency,
                                         ExecutionDate=t.ExecutionDate, InstrumentId=t.InstrumentId, OrderId=t.OrderId,
                                         Price=t.Price, Side=t.Side, Track=t.Track, TradeDate=t.TradeDate,
                                         TradeSystemId=t.TradeSystemId, TradeType=t.TradeType, TsTradeId=t.TsTradeId,
                                         ValueDate=t.ValueDate, ExternalClientId=t.ExternalClientId) for t in total_trades]

        live_trades_sql_footer = """ ON DUPLICATE KEY UPDATE `Amount`=VALUES(`Amount`) ,`Cancelled`=VALUES(`Cancelled`) ,
            `ClientOrderId`=VALUES(`ClientOrderId`) , 	`Closed`=VALUES(`Closed`) , 	`Commission`=VALUES(`Commission`) ,
            `CommissionCurrency`=VALUES(`CommissionCurrency`) , 	`ExecutionDate`=VALUES(`ExecutionDate`) , 	`InstrumentId`=VALUES(`InstrumentId`) ,
            `OrderId`=VALUES(`OrderId`) , 	`Price`=VALUES(`Price`) , 	`Side`=VALUES(`Side`) , 	`Track`=VALUES(`Track`) , 	`TradeDate`=VALUES(`TradeDate`) ,
            `TradeSystemId`=VALUES(`TradeSystemId`) , 	`TradeType`=VALUES(`TradeType`) , 	`TsTradeId`=VALUES(`TsTradeId`) , 	`ValueDate`=VALUES(`ValueDate`) ,
            `ExternalClientId`=VALUES(`ExternalClientId`), `Updated_time`=DATE_SUB(now(),INTERVAL 8 HOUR) """

        async_sql_insert(app=current_app._get_current_object(),header=live_trades_sql_header,values=live_trade_values,footer= live_trades_sql_footer,sql_max_insert=1000)
        #sql_insert_max = 1000
        # for i in range(math.ceil(len(live_trade_values) / sql_insert_max)):
        #
        #
        #     sql_trades_insert = live_trades_sql_header + " , ".join(live_trade_values[i*sql_insert_max :(i+1)*sql_insert_max]) + live_trades_sql_footer
        #     sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
        #     #print(sql_trades_insert)
        #     sql_trades_insert = text(sql_trades_insert) # To make it to SQL friendly text.
        #
        #     raw_insert_result = db.engine.execute(sql_trades_insert)


    return_val = [{k: "{}".format(t[k]) for k in t} for t in total_trades]
    #print(return_val)

    # # Need to update Run time on SQL Update table.
    async_update_Runtime(app=current_app._get_current_object(), Tool="CFH_Live_Trades")
    #print(return_val)

    return json.dumps(return_val)


# Want to check and close off account/trades.
@main_app.route('/CFH_Symbol_Update', methods=['GET', 'POST'])
@login_required
def CFH_Soap_Symbol():

    title = "CFH Symbol Update"
    header = "CFH Symbol Update"
    description = Markup("Getting CFH Symbol Details from CFH BO via CFH's SOAP API.<br>Results will be inserted/updated to aaron.cfh_symbol.<br>Update time in table is GMT.")

    return render_template("Standard_Single_Table.html", backgroud_Filename='css/notebook_pen.jpg', Table_name="CFH Symbols", \
                           title=title, ajax_url=url_for('main_app.CFH_Soap_Symbol_ajax'), header=header,
                           description=description, replace_words=Markup(["Today"]))

@main_app.route('/CFH_Symbol_Update_ajax', methods=['GET', 'POST'])
@login_required
def CFH_Soap_Symbol_ajax(update_all=0):  # Optional Parameter, to update from the start should there be a need to.
    # TODO: CFH_Symbol_Update_ajax Minotor Tools Table.

    wsdl_url = "https://ws.cfhclearing.com:8094/ClientUserDataAccess?wsdl"
    session_soap = Session()
    session_soap.auth = HTTPBasicAuth("BG_Michael", current_app.config["CFH_BO_PASSWORD"])
    client = Client(wsdl_url, transport=Transport(session=session_soap))

    database_name = "aaron"
    table_name = "CFH_Symbol"

    total_symbols = client.service.GetInstruments()

    symbol_sql_header = """INSERT INTO {database_name}.{table_name} (`InstrumentId`,	`InstrumentName`,	`InstrumentSymbol`,	`InstrumentType`,	`InstrumentTypeId`,	`IsActive`,	`InstrumentSubType`,
    `InstrumentSubTypeId`,	`Decimals`,	`DecimalsFullPip`,	`SwapDecimals`,	`MinOrderSize`,	`IsWholeLots`,	`ContractFactor`,
    `CurrencyCode`,	`ValueDateConvention`,	`InMiFIRscope`,	`UnderlyingISINs`, Updated_time) VALUES """.format(database_name=database_name, table_name=table_name)

    symbol_values = ",".join([""" ('{InstrumentId}',	'{InstrumentName}',	'{InstrumentSymbol}',	'{InstrumentType}',	'{InstrumentTypeId}',	'{IsActive}',
     '{InstrumentSubType}',	'{InstrumentSubTypeId}',	'{Decimals}',	'{DecimalsFullPip}',	'{SwapDecimals}',	'{MinOrderSize}',	'{IsWholeLots}',
     '{ContractFactor}',	'{CurrencyCode}',	'{ValueDateConvention}',	'{InMiFIRscope}',
     '{UnderlyingISINs}', DATE_SUB(now(),INTERVAL 8 HOUR))""".format(InstrumentId=s.InstrumentId,
                                                                 InstrumentName=s.InstrumentName.replace("'", "''"),
                                                                 InstrumentSymbol=s.InstrumentSymbol,
                                                                 InstrumentType=s.InstrumentType,
                                                                 InstrumentTypeId=s.InstrumentTypeId,
                                                                 IsActive=s.IsActive,
                                                                 InstrumentSubType=s.InstrumentSubType,
                                                                 InstrumentSubTypeId=s.InstrumentSubTypeId,
                                                                 Decimals=s.Decimals,
                                                                 DecimalsFullPip=s.DecimalsFullPip,
                                                                 SwapDecimals=s.SwapDecimals,
                                                                 IsWholeLots=s.IsWholeLots,
                                                                 MinOrderSize=s.MinOrderSize,
                                                                 ContractFactor=s.ContractFactor,
                                                                 CurrencyCode=s.CurrencyCode,
                                                                 ValueDateConvention=s.ValueDateConvention,
                                                                 InMiFIRscope=s.InMiFIRscope,
                                                                 UnderlyingISINs=s.UnderlyingISINs) for s in total_symbols])


    symbol_sql_footer = """ ON DUPLICATE KEY UPDATE `InstrumentName`=VALUES(`InstrumentName`),	`InstrumentType`=VALUES(`InstrumentType`),	`InstrumentTypeId`=VALUES(`InstrumentTypeId`),
    `IsActive`=VALUES(`IsActive`),	`InstrumentSubType`=VALUES(`InstrumentSubType`),	`InstrumentSubTypeId`=VALUES(`InstrumentSubTypeId`),	`Decimals`=VALUES(`Decimals`),
    `DecimalsFullPip`=VALUES(`DecimalsFullPip`),	`SwapDecimals`=VALUES(`SwapDecimals`),	`MinOrderSize`=VALUES(`MinOrderSize`),	`IsWholeLots`=VALUES(`IsWholeLots`),
    `ContractFactor`=VALUES(`ContractFactor`),	`CurrencyCode`=VALUES(`CurrencyCode`),	`ValueDateConvention`=VALUES(`ValueDateConvention`),	`InMiFIRscope`=VALUES(`InMiFIRscope`),
    `UnderlyingISINs`=VALUES(`UnderlyingISINs`), `Updated_time`=DATE_SUB(now(),INTERVAL 8 HOUR)"""


    sql_insert = symbol_sql_header + symbol_values + symbol_sql_footer
    sql_insert = sql_insert.replace("\t", "").replace("\n", "")
    sql_insert = text(sql_insert) # To make it to SQL friendly text.

    raw_insert_result = db.engine.execute(sql_insert)

    # raw_insert_result = db.engine.execute(sql_insert)
    return_val = [{k: "{}".format(t[k]) for k in t} for t in total_symbols]



    return json.dumps(return_val)
#
#
# @main_app.route('/g', methods=['GET', 'POST'])
# def read_from_main_app_g():
#
#
#     # print(main_app.config["MAIL_SERVER"])
#     # print(main_app.config["MAIL_PORT"])
#     # print(main_app.config["MAIL_USE_TLS"])
#     # print(main_app.config["MAIL_USERNAME"])
#     # print(main_app.config["MAIL_PASSWORD"])
#
#     # if "Swap_data" in g:
#     #  print("Swap_data in g")
#     # else:
#     #  print("Swap_data Data? ")
#     return render_template("upload_form.html")


#
# @main_app.route('/h', methods=['GET', 'POST'])
# def read_from_main_app_h():
#
#     # Time = datetime.now().strftime("%H:%M:%S")
#     # for t in range(10):
#     #  SymbolTotalTest.append_field(str(t) + " " + str(Time), FormField(SymbolSwap))
#     # form = SymbolTotalTest()
#     # return render_template("standard_form.html", form=form)
#     return render_template("Standard_Single_Table.html", backgroud_Filename='css/test7.jpg', Table_name="Table1", title="Table", ajax_url=url_for('main_app.LP_Margin_UpdateTime'))






def markup_swaps(Val, positive_markup, negative_markup ):
    val = Val
    if val >= 0:  # Positive!
        markup_percentage = float(positive_markup)
        val = val * (100 - markup_percentage) / 100
    else:
        markup_percentage = float(negative_markup)
        val = val * (100 + markup_percentage) / 100
    return val


def Check_Float(element):
    try:
        float(element)
        return True
    except ValueError:
        print ("Not a float")
        return False

# Need to have a generic page that does all the SQL Query RAW



@main_app.route('/is_prime')
@login_required
def is_prime_query_AccDetails():    # Query Is Prime

    # is_prime_query_AccDetails_json()
    return render_template("Is_prime_html.html",header="IS Prime Account Details")


@main_app.route('/is_prime_return_json', methods=['GET', 'POST'])    # Query Is Prime, Returns a Json.
@login_required
def is_prime_query_AccDetails_json():
    API_URL_BASE = 'https://api.isprimefx.com/api/'
    USERNAME = "aaron.lim@blackwellglobal.com"
    PASSWORD = "resortshoppingstaff"

    # login and extract the authentication token
    response = requests.post(API_URL_BASE + 'login', \
    headers={'Content-Type': 'main_application/json'}, data=json.dumps({'username': USERNAME, 'password': PASSWORD}), verify=False)
    token = response.json()['token']
    headers = {'X-Token': token}

    response = requests.get(API_URL_BASE + '/risk/positions', headers=headers, verify=False)
    dict_response = json.loads(json.dumps((response.json())))

    account_dict = dict()

    for s in dict_response:
        if "groupName" in s and s["groupName"] ==  "Blackwell Global BVI Risk 2":   # Making sure its the right account.
            account_dict = s
            lp = "Is Prime Terminus"
            account_balance = s["collateral"]
            account_margin_use = s["requirement"]
            account_floating = s["unrealisedPnl"]
            account_equity = s["netEquity"]
            account_free_margin = s["preDeliveredCollateral"]
            account_credit = 0
            account_margin_call = 100
            account_stop_out = 0
            account_stop_out_amount = 0

            sql_insert = "INSERT INTO  aaron.`bgi_hedge_lp_summary` \
            (`lp`, `deposit`, `pnl`, `equity`, `total_margin`, `free_margin`, `credit`, `margin_call(E/M)`, `stop_out(E/M)`, `stop_out_amount`, `updated_time(SGT)`) VALUES" \
                         " ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',NOW()) ".format(lp, account_balance, account_floating, account_equity, account_margin_use, account_free_margin, \
                                                                                                  account_credit, account_margin_call,account_stop_out, account_stop_out_amount)
            SQL_LP_Footer = " ON DUPLICATE KEY UPDATE deposit = VALUES(deposit), pnl = VALUES(pnl), equity = VALUES(equity), total_margin = VALUES(total_margin), free_margin = VALUES(free_margin), `updated_time(SGT)` = VALUES(`updated_time(SGT)`), credit = VALUES(credit), `margin_call(E/M)` = VALUES(`margin_call(E/M)`),`stop_out(E/M)` = VALUES(`stop_out(E/M)`) ";

            sql_insert += SQL_LP_Footer

            # print(sql_insert)
            raw_insert_result = db.engine.execute(sql_insert)
            # raw_insert_result

    for s in account_dict:
        if isinstance(account_dict[s], float) or isinstance(account_dict[s], int):
            account_dict[s] = "{:,.2f}".format(account_dict[s])

    account_dict["Updated_Time(SGT)"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #table = create_table_fun([account_dict])

    return json.dumps(account_dict)


@main_app.route('/MT5_Modify_Trade', methods=['GET', 'POST'])
@login_required
def Modify_MT5_Trades():    # To upload the Files, or post which trades to delete on MT5
    form = MT5_Modify_Trades_Form()
    # file_form = File_Form()

    if request.method == 'POST':
        print("POST Method")
        if form.validate_on_submit():
            MT5_Login = form.MT5_Login.data  # Get the Data.
            MT5_Deal_Num = form.MT5_Deal_Num.data
            MT5_Comment = form.MT5_Comment.data
            MT5_Action = form.MT5_Action.data
            print("Login: {login}, Deal: {deal}, Action: {action}, New Comment: {new_comment}".format(login=MT5_Login, deal=MT5_Deal_Num, action=MT5_Action, new_comment=MT5_Comment))
        else:
            print("Not Validated. ")

        #
        # if file_form.validate_on_submit():
        #  record_dict = request.get_records(field_name='upload', name_columns_by_row=0)
        #  print(record_dict)
            # df_uploaded_data = pd.DataFrame(record_dict)
            # print(df_uploaded_data)
    return render_template("MT5_Modify_Trades.html", form=form, header="MT5 Modify Trades", description="MT5 Modify Trades")


@main_app.route('/Get_Live1_MT4User')
@login_required
def Live1_MT4_Users():
    return Live_MT4_Users(1)

@main_app.route('/Get_Live2_MT4User')
@login_required
def Live2_MT4_Users():
    return Live_MT4_Users(2)

@main_app.route('/Get_Live3_MT4User')
@login_required
def Live3_MT4_Users():
    return Live_MT4_Users(3)

@main_app.route('/Get_Live5_MT4User')
@login_required
def Live5_MT4_Users():
    return Live_MT4_Users(5)


@main_app.route('/sent_file/Risk_Download')
@login_required
def Risk_Download_Page():    # To upload the Files, or post which trades to delete on MT5
    return render_template("Risk_Download_Page.html",header=" ",title="Risk Download Page")


@main_app.route('/ABook_Match_Trades')
@login_required
def ABook_Matching():    # To upload the Files, or post which trades to delete on MT5

    return render_template("A_Book_Matching.html",header="A Book Matching", title="LP/MT4 Position")


@main_app.route('/ABook_Match_Trades_Position', methods=['GET', 'POST'])
@login_required
def ABook_Matching_Position_Vol():    # To upload the Files, or post which trades to delete on MT5

    mismatch_count = [10,15]
    #mismatch_count_1 = 1   # Notify when mismatch has lasted 1st time.
    #mismatch_count_2 = 15   # Second notify when mismatched has lasted a second timessss
    cfh_soap_query_count = [5]   # Want to fully quiery and update from CFH when mismatches reaches this.

    sql_query = text("""SELECT SYMBOL,COALESCE(vantage_LOT,0) AS Vantage_lot,COALESCE(CFH_Position,0) AS CFH_Lots ,COALESCE(api_LOT,0) AS API_lot,COALESCE(offset_LOT,0) AS Offset_lot,COALESCE(vantage_LOT,0)+ COALESCE(CFH_Position,0)-COALESCE(api_LOT,0)+COALESCE(offset_LOT,0) AS Lp_Net_Vol
        ,COALESCE(S.mt4_NET_VOL,0) AS MT4_Net_Vol,COALESCE(vantage_LOT,0)+COALESCE(CFH_Position,0)-COALESCE(api_LOT,0)+COALESCE(offset_LOT,0)-COALESCE(S.mt4_NET_VOL,0) AS Discrepancy 
        FROM test.core_symbol
        LEFT JOIN
        (SELECT mt4_symbol AS vantage_SYMBOL,ROUND(SUM(vantage_LOT),2) AS vantage_LOT FROM 
            (SELECT coresymbol,position/core_symbol.CONTRACT_SIZE AS vantage_LOT,mt4_symbol FROM test.`vantage_live_trades` 
            LEFT JOIN test.vantage_margin_symbol ON vantage_live_trades.coresymbol = vantage_margin_symbol.margin_symbol 
            LEFT JOIN test.core_symbol ON vantage_margin_symbol.mt4_symbol = core_symbol.SYMBOL 
            WHERE CONTRACT_SIZE>0) 
        AS B GROUP BY mt4_symbol) AS Y ON core_symbol.SYMBOL = Y.vantage_SYMBOL
		    LEFT JOIN
        (SELECT cfh_bgi_symbol.mt4_symbol as `CFH_Symbol`, position * (1/core_symbol.CONTRACT_SIZE) AS CFH_Position
            FROM aaron.`cfh_live_position_fix` 
            LEFT JOIN aaron.cfh_bgi_symbol ON cfh_live_position_fix.Symbol = cfh_bgi_symbol.CFH_Symbol
            LEFT JOIN test.core_symbol ON core_symbol.SYMBOL = cfh_bgi_symbol.mt4_symbol  
            WHERE CONTRACT_SIZE>0
        ) as CFH ON core_symbol.SYMBOL = CFH.CFH_Symbol
        LEFT JOIN
        (SELECT coresymbol AS api_SYMBOL,ROUND(SUM(api_LOT),2) AS api_LOT FROM (SELECT bgimargin_live_trades.margin_id,bgimargin_live_trades.coresymbol,position/core_symbol.CONTRACT_SIZE AS api_LOT FROM test.`bgimargin_live_trades` 
        LEFT JOIN test.core_symbol ON bgimargin_live_trades.coresymbol = core_symbol.SYMBOL WHERE CONTRACT_SIZE>0) AS B GROUP BY coresymbol) AS K ON core_symbol.SYMBOL = K.api_SYMBOL
        LEFT JOIN
        (SELECT SYMBOL AS offset_SYMBOL,ROUND(SUM(LOTS),2) AS offset_LOT FROM test.offset_live_trades GROUP BY SYMBOL) AS P ON core_symbol.SYMBOL = P.offset_SYMBOL
        LEFT JOIN
        (SELECT SYMBOL AS mt4_SYMBOL,ROUND(SUM(VOL),2) AS mt4_NET_VOL FROM
        (SELECT 'live1' AS LIVE,SUM(CASE WHEN (mt4_trades.CMD = 0) THEN mt4_trades.VOLUME*0.01 WHEN (mt4_trades.CMD = 1) THEN mt4_trades.VOLUME*(-1)*0.01 ELSE 0 END) AS VOL, 
        (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END) AS `SYMBOL` 
        FROM live1.mt4_trades WHERE ((mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live1.a_group)) OR (LOGIN IN (SELECT LOGIN FROM live1.a_login )))
        AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' AND CMD < 2
        GROUP BY (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END)
        UNION
        SELECT 'live2' AS LIVE,SUM(CASE WHEN (mt4_trades.CMD = 0) THEN mt4_trades.VOLUME*0.01 WHEN (mt4_trades.CMD = 1) THEN mt4_trades.VOLUME*(-1)*0.01 ELSE 0 END) AS VOL, 
        (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END) AS `SYMBOL` 
        FROM live2.mt4_trades WHERE ((mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live2.a_group)) OR (LOGIN IN (SELECT LOGIN FROM live2.a_login )) OR LOGIN = '9583' OR LOGIN = '9615' OR LOGIN = '9618' OR (mt4_trades.`GROUP` LIKE 'A_ATG%' AND VOLUME > 1501))
        AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' AND CMD < 2 GROUP BY (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END)
        UNION
        SELECT 'live3' AS LIVE,SUM(CASE WHEN (mt4_trades.CMD = 0) THEN mt4_trades.VOLUME*0.01 WHEN (mt4_trades.CMD = 1) THEN mt4_trades.VOLUME*(-1)*0.01 ELSE 0 END) AS VOL, 
        (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END) AS `SYMBOL` 
        FROM live3.mt4_trades WHERE (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live3.a_group)) 
        AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' AND CMD < 2 GROUP BY (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END)
        UNION
        SELECT 'live5' AS LIVE,SUM(CASE WHEN (mt4_trades.CMD = 0) THEN mt4_trades.VOLUME*0.01 WHEN (mt4_trades.CMD = 1) THEN mt4_trades.VOLUME*(-1)*0.01 ELSE 0 END) AS VOL, 
        (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END) AS `SYMBOL` 
        FROM live5.mt4_trades WHERE (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.a_group))
        AND LENGTH(mt4_trades.SYMBOL)>0 AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' AND CMD < 2 GROUP BY (CASE WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%y' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'y',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%q' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'q',1) 
        WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%`' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(SYMBOL,'.',2),'`',1) WHEN SUBSTRING_INDEX(SYMBOL,'.',2) LIKE '.%' THEN SUBSTRING_INDEX(SYMBOL,'.',2) ELSE LEFT(SYMBOL,6) END)
        ) AS X GROUP BY SYMBOL) S ON core_symbol.SYMBOL = S.mt4_SYMBOL ORDER BY ABS(Discrepancy) DESC, ABS(MT4_Net_Vol) DESC, SYMBOL""")

    curent_result = Query_SQL_db_engine(sql_query)  # Function to do the Query and return zip dict.


    # curent_result[10]["Discrepancy"] = 0.1  # Artificially induce a mismatch
    # print("request.method: {}".format(request.method))
    # print("Len of request.form: {}".format(len(request.form)))

    ## Need to check if the post has data. Cause from other functions, the POST details will come thru as well.
    if request.method == 'POST' and len(request.form) > 0:    # If the request came in thru a POST. We will get the data first.
        #print("Request A Book Matching method: POST")

        post_data = dict(request.form)  # Want to do a copy.
        #print(post_data)

        # Check if we need to send Email
        Send_Email_Flag =  int(post_data["send_email_flag"]) if ("send_email_flag" in post_data) \
                                                                   and (isinstance(post_data['send_email_flag'], str)
                                                                        and isfloat(post_data['send_email_flag'])) else 0

        # if "MT4_LP_Position_save" in post_data:
        #     print("MT4_LP_Position_save in post_data")
        # else:
        #     print("MT4_LP_Position_save Not in post_data")
        #
        # if isinstance(post_data['MT4_LP_Position_save'], str):
        #     print("post_data['MT4_LP_Position_save'] is str")
        # else:
        #     print("post_data['MT4_LP_Position_save'] is NOT str: {}".format(type(post_data['MT4_LP_Position_save'])))
        #
        #
        # if is_json(post_data["MT4_LP_Position_save"]):
        #     print('post_data["MT4_LP_Position_save"] is json')
        # else:
        #     print('post_data[" MT4_LP_Position_save"] is not json: {}'.format())

        # Check for the past details.
        # Should be stored in Javascript, and returned back Via post.
        # Will contain all the Zeros as well.
        # # Example below:
        #{'send_email_flag': '1',  'MT4_LP_Position_save': '[{"SYMBOL":"GBPCAD","Vantage_lot":0,"CFH_Lots":0.01,"API_lot":0,"Offset_lot":0,"Lp_Net_Vol":0.01,
        # "MT4_Net_Vol":0,"Discrepancy":0.01,"Mismatch_count":15},{"SYMBOL":"XAUUSD","Vantage_lot":0,"CFH_Lots":-10.26,"API_lot":0,"Offset_lot":0,"Lp_Net_Vol":-10.26,"MT4_Net_Vol":-10.26,
        # "Discrepancy":0,"Mismatch_count":0},{"SYMBOL":"GBPUSD","Vantage_lot":0,"CFH_Lots":-0.07,"API_lot":0,"Offset_lot":0,"Lp_Net_Vol":-0.07,"MT4_Net_Vol":-0.07,"Discrepancy":0,"Mismatch_count":0},
        # {"SYMBOL":"AUDNZD","Vantage_lot":0,"CFH_Lots":0.04,"API_lot":0,"Offset_lot":0,"Lp_Net_Vol":0.04,"MT4_Net_Vol":0.04,"Discrepancy":0,"Mismatch_count":0},{"SYMBOL":"CHFJPY","Vantage_lot":0,
        # "CFH_Lots":0.02,"API_lot":0,"Offset_lot":0,"Lp_Net_Vol":0.02,"MT4_Net_Vol":0.02,"Discrepancy":0,"Mismatch_count":0},{"SYMBOL":"USDJPY","Vantage_lot":0,"CFH_Lots":-0.02,"API_lot":0,"Offset_lot":0,
        # "Lp_Net_Vol":-0.02,"MT4_Net_Vol":-0.02,"Discrepancy":0,"Mismatch_count":0}]'}

        Past_Details = json.loads(post_data["MT4_LP_Position_save"]) if ("MT4_LP_Position_save" in post_data) \
                                                                           and (isinstance(post_data['MT4_LP_Position_save'], str)) \
                                                                           and is_json(post_data["MT4_LP_Position_save"]) \
                                                                            else []
        # If we want to send all the total position
        send_email_total = int(post_data["send_email_total"][0]) if ("send_email_total" in post_data) \
                                                                   and (isinstance(post_data['send_email_total'], list)) else 0

        # Variables to return.
        Play_Sound = 0                                  # To play sound if needed

        #print("Past Details: {}".format(Past_Details))
        # To Calculate the past (Previous result) Mismatches
        Past_discrepancy = dict()
        for pd in Past_Details:
            if "SYMBOL" in pd and "Discrepancy" in pd.keys() and pd["Discrepancy"] != 0:    # If the keys are there.
                    Past_discrepancy[pd["SYMBOL"]] = pd["Mismatch_count"] if "Mismatch_count" in pd else 1  # Want to get the count. Or raise as 1.


        # #To Artificially induce a mismatch
        # curent_result[0]["Discrepancy"] = 0.01
        # curent_result[1]["Discrepancy"] = 0.01
        # curent_result[2]["Discrepancy"] = 0.01

        # To tally off with current mismatches. If there are, add 1 to count. Else, Zero it.
        for d in curent_result:
            if "Discrepancy" in d.keys():
                if d["Discrepancy"] != 0:   # There are mismatches Currently.
                    d["Mismatch_count"] = 1 if d['SYMBOL'] not in Past_discrepancy else Past_discrepancy[d['SYMBOL']] + 1
                else:
                    d["Mismatch_count"] = 0

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




                # If there are mismatches, first thing to do is to update CFH. All trades.
                # Some older trades might have been closed.
                if any([d["Mismatch_count"] in cfh_soap_query_count for d in Notify_Mismatch]):
                    chf_fix_details_ajax()  # Want to update CFH Live Trades.
                    #TODO: Update Vantage Live trades too, if possible.

                    #CFH_Live_Position_ajax(update_all=1)    # Want to update all trades from CFH
                    #print("Mismatch. Will Send SOAP to refresh all trades.")


            Tele_Message = "<b>MT4/LP Position</b> \n\n"  # To compose Telegram outgoing message
            email_html_body = "Hi, <br><br>";
            Email_Title_Array = []

            # If there are mismatch count that are either mismatch_count_1 or mismatch_count_2, we will send the email.
            if any([ d["Mismatch_count"] in mismatch_count for d in Notify_Mismatch]):    # If there are to be notified.

                Play_Sound += 1  # Raise the flag to play sound.
                Notify_mismatch_table_html = Array_To_HTML_Table(list(Notify_Mismatch[0].keys()), [list(d.values()) for d in Notify_Mismatch])
                Email_Title_Array.append("Mismatch")
                email_html_body +=  "There is a mismatch for A-Book LP/MT4 trades.<br>{}".format(Notify_mismatch_table_html)


                # Want to find the potential mismatch trades from MT4 and Bridge
                ##bridge_trades = Mismatch_trades_bridge(symbol=Current_discrepancy, hours=7, mins=16)
                ##mt4_trades = Mismatch_trades_mt4(symbol=Current_discrepancy, hours=7, mins=16)

                bridge_trades = Mismatch_trades_bridge(symbol=Current_discrepancy, hours=8, mins=15)
                mt4_trades = Mismatch_trades_mt4(symbol=Current_discrepancy, hours=7, mins=15)

                bridge_trades_html_table = Array_To_HTML_Table(Table_Header = bridge_trades[0], Table_Data=bridge_trades[1]) if len(bridge_trades[1]) > 0 else "- No Trades Found for that time perid.\n"
                mt4_trades_html_table = Array_To_HTML_Table(Table_Header=mt4_trades[0], Table_Data=mt4_trades[1]) if len(mt4_trades[1]) > 0 else "- No Trades Found for that time perid.\n"

                email_html_body += "<br><b><u>MT4 trades</u></b> around the time:<br>{mt4_table}<br><br><b><u>Bridge trades</u></b> around that time:<br>{bridge_table}<br>".format(
                    mt4_table=mt4_trades_html_table,bridge_table=bridge_trades_html_table)

                # print(Notify_Mismatch)
                Tele_Message += "<pre>{} Mismatch</pre>\n {}".format(len(Current_discrepancy), " ".join(["{}: {} Lots, {} Mins.\n".format(c["SYMBOL"], c["Discrepancy"], c["Mismatch_count"]) for c in Notify_Mismatch]))

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



            if Send_Email_Flag == 1 and len(Email_Title_Array) > 0:    # If there are things to be sent, we determine by looking at the title array
                api_update_details = json.loads(LP_Margin_UpdateTime())  # Want to get the API/LP Update time.

                email_html_body +=  "The API/LP Update timings:<br>" + Array_To_HTML_Table(
                    list(api_update_details[0].keys()), [list(api_update_details[0].values())], ["Update Slow"])
                email_html_body += "This Email was generated at: SGT {}.<br><br>Thanks,<br>Aaron".format(
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                # Send the email
                async_send_email(EMAIL_LIST_ALERT, [], "A Book Position ({}) ".format("/ ".join(Email_Title_Array)),
                       Email_Header + email_html_body + Email_Footer, [])

                # Send_Email(EMAIL_LIST_ALERT, [], "A Book Position ({}) ".format("/ ".join(Email_Title_Array)), Email_Header + email_html_body + Email_Footer, [])

                # Want to send to telegram the timing that the API was updated.
                api_update_time = api_update_details[0] if len(api_update_details) else {}
                api_update_str = "\n<pre>Update time</pre>\n" + "\n".join(["{k} : {d}".format(k=k, d=d.replace("<br>", " ")) for k,d in api_update_time.items()]) \
                                        if len(api_update_details) else ""

                Tele_Message += api_update_str

                # Send the Telegram message.
                async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Message, TELE_CLIENT_ID, Parse_mode=telegram.ParseMode.HTML)

        # '[{"Vantage_Update_Time": "2019-09-17 16:54:20", "BGI_Margin_Update_Time": "2019-09-17 16:54:23"}]'

    #print("Current Results: {}".format(curent_result))
    return_result = {"current_result":curent_result, "Play_Sound": Play_Sound}   # End of if/else. going to return.

    return json.dumps(return_result)


@main_app.route('/ABook_LP_Details', methods=['GET', 'POST'])
@login_required
def ABook_LP_Details():    # LP Details. Balance, Credit, Margin, MC/SO levels. Will alert if email is set to send.
                            # Checks Margin against MC/SO values, with some buffer as alert.

    sql_query = text("""SELECT lp, deposit, credit, pnl, equity, total_margin, free_margin, 
		ROUND((credit + deposit + pnl),2) as EQUITY, 
		ROUND(100 * total_margin / (credit + deposit + pnl),2) as `Margin/Equity (%)` ,
		margin_call as `margin_call (M/E)` , stop_out as `stop_out (M/E)`,
			  COALESCE(`stop_out_amount`, ROUND(  100* (`total_margin`/`stop_out`) ,2)) as `STOPOUT AMOUNT`,
		ROUND(`equity` -  COALESCE(`stop_out_amount`, 100* (`total_margin`/`stop_out`) ),2) as `available`,
		updated_time 
		FROM aaron.lp_summary ORDER BY LP DESC""")  # Need to convert to Python Friendly Text.
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()
    # result_data_json_parse = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_data_json_parse = [[time_difference_check(a) if isinstance(a, datetime.datetime) else a for a in d] for d in
                           result_data]  # correct The decimal.Decimal class to float.

    result_col = raw_result.keys()
    return_result = [dict(zip(result_col,d)) for d in result_data_json_parse]


    LP_Position_Show_Table = [] # to store the LP details in a more read-able sense

    Tele_Margin_Text = "*LP Margin Issues.*\n"    # To Compose Telegram text for Margin issues.
    margin_attention_flag = 0
    Margin_MC_Flag = 0
    Margin_SO_Flag = 0
    Play_Sound = 0  # For return to AJAX


    for i,lp in enumerate(return_result):    # Want to re-arrange.
        loop_buffer = dict()
        loop_buffer["LP"] = lp["lp"] if "lp" in lp else None

        Lp_MC_Level = lp["margin_call (M/E)"] if "margin_call (M/E)" in lp else None
        Lp_SO_Level = lp["stop_out (M/E)"] if "stop_out (M/E)" in lp else None
        Lp_Margin_Level = lp["Margin/Equity (%)"]  if "Margin/Equity (%)" in lp else None

        # Want to induce an error
        #Lp_Margin_Level = lp["Margin/Equity (%)"] + 105  if "Margin/Equity (%)" in lp else None

        loop_buffer["BALANCE"] = dict()
        loop_buffer["BALANCE"]["DEPOSIT"] = "$ {:,.2f}".format(float(lp["deposit"])) if "deposit" in lp else None
        loop_buffer["BALANCE"]["CREDIT"] = "$ {:,.2f}".format(float(lp["credit"])) if "credit" in lp else None
        loop_buffer["BALANCE"]["PNL"] = "$ {:,.2f}".format(float(lp["pnl"])) if "pnl" in lp else None
        loop_buffer["BALANCE"]["EQUITY"] = "$ {:,.2f}".format(float(lp["equity"])) if "equity" in lp else None

        loop_buffer["MARGIN"] = dict()
        loop_buffer["MARGIN"]["TOTAL_MARGIN"] = "${:,.2f}".format(float(lp["total_margin"])) if "total_margin" in lp else None
        loop_buffer["MARGIN"]["FREE_MARGIN"] = "${:,.2f}".format(float(lp["free_margin"])) if "free_margin" in lp else None

        # Checking Margin Levels.
        if Lp_Margin_Level >= (Lp_SO_Level - LP_MARGIN_ALERT_LEVEL):    # Check SO Level First.
            Margin_SO_Flag +=1
            loop_buffer["MARGIN/EQUITY (%)"] = "SO Alert: {:.2f}%".format(Lp_Margin_Level)
            Tele_Margin_Text += "_SO Alert_: {} margin is at {:.2f}.\n".format( loop_buffer["LP"], Lp_Margin_Level)
        elif Lp_Margin_Level >= Lp_MC_Level:                            # Check Margin Call Level
            Margin_MC_Flag += 1
            loop_buffer["MARGIN/EQUITY (%)"] = "Margin Call: {:.2f}%".format(Lp_Margin_Level)
            Tele_Margin_Text += "_MC Alert_: {} margin is at {}. MC/SO: {:.2f}/{:.2f}\n".format(loop_buffer["LP"], Lp_Margin_Level,Lp_MC_Level,Lp_SO_Level)
        elif Lp_Margin_Level >= (Lp_MC_Level - LP_MARGIN_ALERT_LEVEL):  # Want to start an alert when reaching MC.
            margin_attention_flag += 1
            loop_buffer["MARGIN/EQUITY (%)"] = "Alert: {:.2f}%".format(Lp_Margin_Level)
            Tele_Margin_Text += "_Margin Alert_: {} margin is at {}. MC is at {:.2f}\n".format(loop_buffer["LP"], Lp_Margin_Level, Lp_MC_Level)
        else:
            loop_buffer["MARGIN/EQUITY (%)"] = "{}%".format(Lp_Margin_Level)

        loop_buffer["MC/SO/AVAILABLE"] = dict()
        loop_buffer["MC/SO/AVAILABLE"]["MARGIN_CALL (M/E)"] = "{:.2f}".format(Lp_MC_Level)
        loop_buffer["MC/SO/AVAILABLE"]["STOP_OUT (M/E)"] = "{:.2f}".format(Lp_SO_Level)
        loop_buffer["MC/SO/AVAILABLE"]["STOPOUT AMOUNT"] = "$ {:,.2f}".format(float(lp["STOPOUT AMOUNT"])) if "STOPOUT AMOUNT" in lp else None
        loop_buffer["MC/SO/AVAILABLE"]["AVAILABLE"] = "$ {:,.2f}".format(float(lp["available"])) if "available" in lp else None

        loop_buffer["UPDATED_TIME"] = lp["updated_time"] if "updated_time" in lp else None

        LP_Position_Show_Table.append(loop_buffer)
        # print(loop_buffer)

    if request.method == 'POST':
        post_data = dict(request.form)
        #print(post_data)

        # Get variables from POST.
        Send_Email_Flag = int(post_data["send_email_flag"]) if ("send_email_flag" in post_data) \
                               and (isinstance(post_data['send_email_flag'], str)) else 0

        lp_attention_email_count = int(post_data["lp_attention_email_count"]) if ("lp_attention_email_count" in post_data) \
                                and (isinstance(post_data['lp_attention_email_count'], str)) else 0

        lp_mc_email_count = int(post_data["lp_mc_email_count"]) if ("lp_mc_email_count" in post_data) \
                                 and (isinstance(post_data['lp_mc_email_count'], str)) else 0


        lp_time_issue_count = int(post_data["lp_time_issue_count"]) if ("lp_time_issue_count" in post_data) \
                                 and (isinstance(post_data['lp_time_issue_count'], str)) else -1


        lp_so_email_count = int(post_data["lp_so_email_count"]) if ("lp_so_email_count" in post_data) \
                                and (isinstance(post_data['lp_so_email_count'], str)) else -1



        if Send_Email_Flag == 1:    # Want to update the runtime table to ensure that tool is running.
            async_update_Runtime(app=current_app._get_current_object(), Tool="LP_Details_Check")


        Tele_Message = "*LP Details* \n"  # To compose Telegram outgoing message

        # Checking if there are any update time that are slow. Returns a Bool
        update_time_slow = any([[True for a in d if (isinstance(a, datetime.datetime) and abs((a-datetime.datetime.now()).total_seconds()) > TIME_UPDATE_SLOW_MIN*60)] for d in result_data])


        if update_time_slow: # Want to send an email out if time is slow.
            if lp_time_issue_count == 0:    # For time issue.
                if Send_Email_Flag == 1:
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])

                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject="LP Details not updating",
                                     HTML_Text="{}Hi,<br><br>LP Details not updating. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".format(
                                         Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,Email_Footer), Attachment_Name=[])


                    LP_Issue_Name = [d['lp'] for d in result_data if ("lp" in d) and ("updated_time" in d) and abs(
                        (d["updated_time"] - datetime.datetime.now()).total_seconds()) > (TIME_UPDATE_SLOW_MIN * 60)]
                    Tele_Message += "_Update Slow_: {}\n".format(", ".join(LP_Issue_Name))

                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Message, TELE_CLIENT_ID)

                Play_Sound+=1   # No matter sending emails or not, we will need to play sound.
                lp_time_issue_count += 1
        else:   # Reset to 0.
            lp_time_issue_count = 0

        #Margin_SO_Flag: 0, Send_Email_Flag: 1, lp_so_email_count: 0, Margin_MC_Flag: 2

        #print("Margin_SO_Flag : {}, lp_so_email_count: {}, Send_Email_Flag:{}".format(Margin_SO_Flag, lp_so_email_count, Send_Email_Flag))
        #print("Margin_MC_Flag : {}, lp_mc_email_count: {}, Send_Email_Flag:{}".format(Margin_MC_Flag, lp_mc_email_count, Send_Email_Flag))
        #print("margin_attention_flag : {}, lp_attention_email_count: {}, Send_Email_Flag:{}".format(margin_attention_flag, lp_attention_email_count, Send_Email_Flag))

        # -------------------- To Check the Margin Levels, and to send email when needed. --------------------------
        if Margin_SO_Flag > 0:   # If there are margin issues. Want to send Alert Out.
            if lp_so_email_count == 0:
                if Send_Email_Flag == 1:
                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Margin_Text, TELE_CLIENT_ID)
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])


                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject = "LP Account approaching SO.",
                                     HTML_Text="{}Hi,<br><br>LP Account margin reaching SO Levels. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".
                                     format(Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Email_Footer),
                                     Attachment_Name=[])


            Play_Sound += 1  # No matter sending emails or not, we will need to play sound.
            # print("Play Sound: {}".format(Play_Sound))
            lp_so_email_count += 1
            lp_mc_email_count = lp_mc_email_count + 1 if lp_mc_email_count <1 else lp_mc_email_count    # Want to raise all to at least 1
            margin_attention_flag = margin_attention_flag + 1 if margin_attention_flag < 1 else margin_attention_flag
        elif Margin_MC_Flag > 0:   # If there are margin issues. Want to send Alert Out.
            if lp_mc_email_count == 0:
                if Send_Email_Flag == 1:
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])

                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject = "LP Account has passed MC Levels.",
                                     HTML_Text="{}Hi,<br><br>LP Account margin reaching SO Levels. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".
                                     format(Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Email_Footer),
                                     Attachment_Name=[])


                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Margin_Text, TELE_CLIENT_ID)
                Play_Sound += 1             # Play sound when MC. Once
            lp_mc_email_count += 1
            lp_so_email_count = 0
        elif margin_attention_flag > 0:  # If there are margin issues. Want to send Alert Out
            if lp_attention_email_count == 0:
                if Send_Email_Flag == 1:
                    async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, Tele_Margin_Text, TELE_CLIENT_ID)
                    LP_position_Table = List_of_Dict_To_Horizontal_HTML_Table(LP_Position_Show_Table, ['Slow', 'Margin Call', 'Alert'])

                    async_send_email(To_recipients=EMAIL_LIST_ALERT, cc_recipients=[],
                                     Subject = "LP Account approaching MC.",
                                     HTML_Text="{}Hi,<br><br>LP Account margin reaching SO Levels. <br>{}<br>This Email was generated at: {} (SGT)<br><br>Thanks,<br>Aaron{}".
                                     format(Email_Header, LP_position_Table, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Email_Footer),
                                     Attachment_Name=[])

                Play_Sound += 1              # Play sound when Nearing MC. Once
            lp_attention_email_count += 1
            lp_so_email_count = 0
            lp_mc_email_count = 0

        else:       # Clear all flags.
            lp_so_email_count = 0
            lp_mc_email_count = 0
            lp_attention_email_count = 0
       # , "Alert", "Margin Call", "SO Attention"

            # return "Error:lalalala"
    return json.dumps({"lp_attention_email_count": lp_attention_email_count, "Play_Sound": Play_Sound, "current_result": LP_Position_Show_Table, "lp_time_issue_count": lp_time_issue_count,
                       "lp_so_email_count": lp_so_email_count, "lp_mc_email_count": lp_mc_email_count})



@main_app.route('/LP_Margin_UpdateTime', methods=['GET', 'POST'])
@login_required
def LP_Margin_UpdateTime():     # To query for LP/Margin Update time to check that it's being updated.
    # TODO: To add in sutible alert should this stop updating and such.
    # TODO: Date return as String instead of datetime. Cannot compare the date to notify when it's slow.


    sql_query = text("""select
    COALESCE(Vantage_Update_Time, 'No Open Trades') as Vantage_Update_Time,
    COALESCE(BGI_Margin_Updated_Time, 'No Open Trades') as BGI_Margin_Update_Time,
    COALESCE(CFH_Updated_Time, 'No Open Trades') as CFH_Updated_Time
    from
    (select min(updated_time) as Vantage_Update_Time from test.vantage_live_trades where position != 0) as V,
    (select updated_time as BGI_Margin_Updated_Time from test.bgimargin_live_trades where coresymbol = 'update') as BGI,
    (select min(updated_time) as CFH_Updated_Time from aaron.cfh_live_position_fix) as CFH""")

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()

    result_data_json_parse = [[try_string_to_datetime(a)  for a in d] for d in result_data]
    # result_data_json_parse = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_data_json_parse = [[time_difference_check(a) if isinstance(a, datetime.datetime) else a for a in d] for d in
                              result_data_json_parse]  # correct The decimal.Decimal class to float.

    result_col = raw_result.keys()
    # Want to do a time check.
    result_data_json_parse = [time_difference_check(d) if isinstance(d, datetime.datetime) else d for d in result_data_json_parse]
    return_result = [dict(zip(result_col,d)) for d in result_data_json_parse]

    return json.dumps(return_result)

# Test if the string will return as date. if not, return string.
def try_string_to_datetime(sstr):
    try:
        d = datetime.datetime.strptime(sstr, '%Y-%m-%d %H:%M:%S')
        return d
    except:
        return sstr



@main_app.route('/CFH/Details')
@login_required
def cfh_details():
    #TODO: Add this into a blue print.
    #loop = asyncio.new_event_loop()

    description = Markup("Pull CFH Details.")
    return render_template("Standard_Multi_Table.html", backgroud_Filename='css/Mac_table_user.jpeg', Table_name=["CFH Account Details", "CFH Live Position"], \
                           title="CFH Details", ajax_url=url_for('main_app.chf_fix_details_ajax'),setinterval=30,
                           description=description, replace_words=Markup(["Today"]))



@main_app.route('/CFH/Details_ajax', methods=['GET', 'POST'])
@login_required
def chf_fix_details_ajax(update_tool_time=1):     # Return the Bloomberg dividend table in Json.

    datetime_now = datetime.datetime.utcnow()
    datetime_now.weekday() # 0 - monday


    if cfh_fix_timing() == False:  # Want to check if CFH Fix still running.
        return_data = [[{"Comment": "Out of CFH Fix timing. From UTC Sunday 2215 to Friday 2215"}],
                       [{"Comment": "Out of CFH Fix timing. From UTC Sunday 2215 to Friday 2215"}]]
        return json.dumps(return_data)


    # Get the Position and Info from CFH FIX.
    [account_info, account_position] = CFH_Position_n_Info()

    if len(account_info) == 0 : # If there are no return.
        return_data = [[{"Error": "No Return Value"}], [{"Error": "No Return Value"}]]
        return json.dumps(return_data)

    fix_position_sql_update(account_position)   # Will append the position to SQL. Will Zero out any others.
    cfh_account_position = [{"Symbol": k, "Position" : d} for k, d in account_position.items()]


    # Now, to calculate the Balance and such. Will put into SQL as well.
    lp = "CFH"
    deposit = (float(account_info["Balance"]) if "Balance" in account_info else 0) + \
                (float(account_info["ClosedPL"]) if "ClosedPL" in account_info else 0)

    pnl = float(account_info["OpenPL"]) if "OpenPL" in account_info else 0

    equity = (float(account_info["Balance"]) if "Balance" in account_info else 0) +\
             (float(account_info["ClosedPL"]) if "ClosedPL" in account_info else 0) + \
             (float(account_info["OpenPL"]) if "OpenPL" in account_info else 0) + \
             (float(account_info["CreditLimit"]) if "CreditLimit" in account_info else 0)

    credit = float(account_info["SecurityDeposit"]) if "SecurityDeposit" in account_info else 0
    #credit =  account_info['CreditLimit']  if 'CreditLimit' in account_info else 0
    account_info['equity'] = equity


    total_margin = account_info['MarginRequirement'] if 'MarginRequirement' in account_info else 0
    free_margin = account_info['AvailableForMarginTrading'] if 'AvailableForMarginTrading' in account_info else 0


    database = "aaron"
    db_table = "lp_summary"
    #db_table = "lp_summary_copy"
    sql_insert = """INSERT INTO {database}.{db_table} (lp, deposit, pnl, equity, total_margin, free_margin, 
            credit, updated_time) VALUES ('{lp}', '{deposit}', '{pnl}', '{equity}', '{total_margin}', 
            '{free_margin}', '{credit}', now()) ON DUPLICATE KEY UPDATE deposit=VALUES(deposit), pnl=VALUES(pnl), 
            total_margin=VALUES(total_margin), equity=VALUES(equity), credit=VALUES(credit),
            free_margin=VALUES(free_margin), Updated_Time=VALUES(Updated_Time) """.format(database=database,
                                    db_table=db_table, lp=lp, deposit=deposit, pnl=pnl, equity=equity,
                                    total_margin="{:.2f}".format(float(total_margin)),
                                    free_margin="{:.2f}".format(float(free_margin)), credit=credit)

    # ASYNC send to SQL.
    async_sql_insert(app=current_app._get_current_object(),header = "", values = [sql_insert], footer="")

    if update_tool_time == 1:
        async_update_Runtime(app=current_app._get_current_object(), Tool="CFH_FIX_Position")


    return_data = [[account_info], cfh_account_position]

    return json.dumps(return_data)




# @main_app.route('/MT4_Commission')
# @login_required
# def MT4_Commission():
#     description = Markup("Swap values uploaded onto MT4/MT5. <br>\
#    Swaps would be charged on the roll over to the next day.<br> \
#     Three day swaps would be charged for FX on weds and CFDs on fri. ")
#     return render_template("Standard_Single_Table.html", backgroud_Filename='css/Faded_car.jpg', Table_name="BGI_Swaps", \
#                            title="BGISwaps", ajax_url=url_for('main_app.Mt4_Commission_ajax'),
#                            description=description, replace_words=Markup([]))
#
#
# @main_app.route('/Mt4_Commission_ajax', methods=['GET', 'POST'])
# @login_required
# def Mt4_Commission_ajax():     # Return the Bloomberg dividend table in Json.
#
#     # start_date = get_working_day_date(datetime.date.today(), -1, 5)
#     sql_query = text("""select mt4_groups.`GROUP`, mt4_securities.`NAME`, mt4_groups.CURRENCY, mt4_groups.DEFAULT_LEVERAGE, mt4_groups.MARGIN_CALL, mt4_groups.MARGIN_STOPOUT, mt4_secgroups.`SHOW`, mt4_secgroups.TRADE, mt4_secgroups.SPREAD_DIFF,
#         mt4_secgroups.COMM_BASE, mt4_secgroups.COMM_TYPE, mt4_secgroups.COMM_LOTS, mt4_secgroups.COMM_AGENT, mt4_secgroups.COMM_AGENT_TYPE, mt4_secgroups.COMM_AGENT_LOTS  from
#         live5.mt4_groups, live5.mt4_secgroups, live5.mt4_securities
#         where mt4_groups.SECGROUPS = mt4_secgroups.SECGROUPS and mt4_secgroups.TYPE = mt4_securities.TYPE and
#         mt4_secgroups.`SHOW` = 1 and mt4_groups.`ENABLE` = 1
#         GROUP BY `GROUP`, mt4_securities.`NAME`""")
#
#     raw_result = db.engine.execute(sql_query)
#     result_data = raw_result.fetchall()
#     result_col = raw_result.keys()
#     return_result = [dict(zip(result_col, d)) for d in result_data]
#
#     return json.dumps(return_result)


# Want to show which clients got recently changed to read only.
# Due to Equity < Balance.
@main_app.route('/Changed_readonly')
@login_required
def Changed_readonly():
    description = Markup("Showing Clients that has been changed to read only in the last 2 working days.")
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/Mac_Coffee.jpg', Table_name="Changed Read Only", \
                           title="Read Only Clients", ajax_url=url_for('main_app.Changed_readonly_ajax', _external=True),
                           description=description, replace_words=Markup(["Today"]))


@main_app.route('/Changed_readonly_ajax', methods=['GET', 'POST'])
@login_required
def Changed_readonly_ajax():

    # Which Date to start with. We want to count back 1 day. 
    start_date = get_working_day_date(datetime.date.today(), weekdays_count= -2)
    sql_query = text("Select * from test.changed_read_only WHERE `date` >= '{}' order by Date DESC".format(start_date.strftime("%Y-%m-%d")))
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result

    result_dict = []
    if len(result_data) > 0:
        # dict of the results
        result_col = raw_result.keys()
        # Clean up the data. Date.
        result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]
        result_dict = [dict(zip(result_col,d)) for d in result_data_clean]
    else:
        result_dict.append({'Result': 'No Clients has been changed since <b>{}</b> MT4 Server Time.'.format(start_date.strftime("%Y-%m-%d"))})

    return json.dumps(result_dict)



@main_app.route('/Monitor_Risk_Tools')
@login_required
def Monitor_Risk_Tools():
    description = Markup("Monitor Risk tools.")
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/clock_left.jpg', Table_name="Risk tools", \
                           title="Risk Tools", ajax_url=url_for('main_app.Monitor_Risk_Tools_ajax', _external=True), setinterval=60,
                           description=description, replace_words=Markup(["Time Slow"]))


# To monitor the Risk Tools, and run them if needed.
# Will need to update it when there are new tools.
@main_app.route('/Monitor_Risk_Tools_ajax', methods=['GET', 'POST'])
@login_required
def Monitor_Risk_Tools_ajax():

    if cfh_fix_timing() == False:  # Want to check if CFH Fix still running.
        return_data = [{"Comment": "Out of CFH Fix timing. From UTC Sunday 2215 to Friday 2215"}]
        return json.dumps(return_data)

    # Which Date to start with. We want to count back 1 day.
    sql_query = text("Select * from aaron.monitor_tool_runtime")
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    return_dict = [dict(zip(result_col, d)) for d in result_data]

    #Need to check the run time against the current time. To check if there has been any issues running it.
    datetime_now = datetime.datetime.now()      # Get the time now.

    #'CFH_Live_Trades': CFH_Soap_Position_ajax,
    #"MT4/LP A Book Check"       : ABook_Matching_Position_Vol,
    #"LP_Details_Check": ABook_LP_Details,

    function_to_call = {'CFH_FIX_Position'          : chf_fix_details_ajax,
                        'ChangeGroup_NoOpenTrades'  : noopentrades_changegroup_ajax,
                        "Equity_protect"            : Equity_protect_Cut_ajax,
                        "Risk_Auto_Cut"             : risk_auto_cut_ajax,
                        "bgi_float_history_save"         : save_BGI_float_Ajax
                        }

    #all_function_return = [d() for k,d in function_to_call.items()]
    #print(all_function_return)

    slow_update_list = []
    resume_update_list = [] # List to store all the updates that had not been updating, but now is updating.

    for i in range(len(return_dict)):   # Loop thru and find out which ones isn't updating.
        if 'Updated_Time' in return_dict[i] and \
                isinstance(return_dict[i]['Updated_Time'], datetime.datetime) and \
                    'Interval' in return_dict[i]:
            #Compute the time difference between the last ran time.
            time_difference =  math.ceil((datetime_now - return_dict[i]["Updated_Time"]).total_seconds())

            # No interval has been set. We can ignore this first.
            # Clean up, and skip to the next Tool for checking.
            if not (("Interval" in return_dict[i]) and return_dict[i]["Interval"] != None):
                return_dict[i]["Last Ran"] = "No Interval Found."
                return_dict[i]["Updated_Time"] = return_dict[i]["Updated_Time"].strftime("%Y-%m-%d %H:%M:%S")
                continue

            # Checks if the tool hasn't been running. Or if there was a slow update. Inputs a Tuple (Tool name, Email sent)
            if (time_difference >= 3*return_dict[i]["Interval"]) or (time_difference >= 120+return_dict[i]["Interval"]):
                return_dict[i]["Last Ran"] = "Time Slow {}".format(time_difference)
                slow_update_list.append((return_dict[i]["Monitor_Tool"],return_dict[i]["Email_Sent"]))
                #print("Time Slow: {}".format(return_dict[i]["Monitor_Tool"]))

            else:
                return_dict[i]["Last Ran"] = time_difference

                # It wasn't updating, but now is.
                if "Interval" in return_dict[i] and time_difference < return_dict[i]["Interval"] and \
                    "Email_Sent" in return_dict[i] and return_dict[i]["Email_Sent"] == "1":
                    resume_update_list.append(return_dict[i]["Monitor_Tool"])

            # Clean up the data. Date.
            return_dict[i]["Updated_Time"] = return_dict[i]["Updated_Time"].strftime("%Y-%m-%d %H:%M:%S")


    #print(slow_update_list)
    #print(resume_update_list)

    if len(slow_update_list) > 0:   # If there are progs that are not running, run them!
        # Run the prog if it's not running
        Catch_return = [function_to_call[d](update_tool_time=0) for (d,e) in slow_update_list if d in function_to_call]
        #print(Catch_return)

    # Only want to update those that have not been updated.
    recent_slow_update = [s[0] for s in slow_update_list if len(s) >= 2 and s[1] == '0']
    if len(recent_slow_update) > 0:

        # Update SQL, set the email to have been sent.
        sql_query = "Update aaron.monitor_tool_runtime set Email_Sent = 1 where Monitor_Tool in ({})".format(",".join(["'{}'".format(r) for r in recent_slow_update]))
        async_sql_insert(app=current_app._get_current_object(),header=sql_query)


        # Need to string it together to send a tele message.
        # Will need to remove special _ character.
        text_to_tele = "\n".join(["- {}".format(d) for d in recent_slow_update]).replace("_"," ")

        async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, "*Risk Tool Slow Update*:\n{}".format(text_to_tele), TELE_CLIENT_ID)
        #print(text_to_tele)

    # For those that have started being updated.
    if len(resume_update_list) > 0:
        sql_query = "Update aaron.monitor_tool_runtime set Email_Sent = 0 where Monitor_Tool in ({})".format(",".join(["'{}'".format(r) for r in resume_update_list]))
        async_sql_insert(app=current_app._get_current_object(),header=sql_query)
        text_to_tele = "\n".join(["- {}".format(r) for r in resume_update_list]).replace("_"," ")
        # Post to Telegram that the tools have been updating again.
        async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, "*Risk Tool Update (Resumed)*:\n{}".format(text_to_tele),
                               TELE_CLIENT_ID)


    return json.dumps(return_dict)


@main_app.route('/Usage')
@login_required
def Computer_Usage():
    description = Markup("Reflects the Server Usage.<br>")
    header = "Server Usage Details."
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/mac_keyboard_side.jpg', Table_name="Server Computer Usage", \
                           title="Server Usage", ajax_url=url_for('main_app.Computer_Usage_Ajax', _external=True), header=header, setinterval=30,
                           description=description, replace_words=Markup(["Today"]))


@main_app.route('/Usage_ajax', methods=['GET', 'POST'])
@login_required
def Computer_Usage_Ajax():
    # gives a single float value
    cpu_per = psutil.cpu_percent()
    # gives an object with many fields
    psutil.virtual_memory()
    # you can convert that object to a dictionary
    mem_usage = dict(psutil.virtual_memory()._asdict())

    if cpu_per >= 90:    # If the CPU usage is more than 90%, we need to know.
        async_Post_To_Telegram(TELE_ID_MTLP_MISMATCH, "*Server Usage:*:{}%".format(cpu_per), TELE_CLIENT_ID)

    return_data = [{"CPU_Usage": "{}%".format(cpu_per), "RAM Memory": "{}%".format(mem_usage['percent'])}]
    return json.dumps(return_data)



@main_app.route('/Convert_rate')
@login_required
def BGI_Convert_Rate():
    description = Markup("BGI Convert Rate.<br>")
    header = "BGI_Convert Rate."
    return render_template("Webworker_Single_Table.html", backgroud_Filename='css/autumn.jpg', Table_name="BGI Convert Rate", \
                           title="BGI Convert Rate", ajax_url=url_for('main_app.BGI_Convert_Rate_Ajax', _external=True), header=header,
                           description=description, replace_words=Markup(["Today"]))


@main_app.route('/Convert_rate_ajax', methods=['GET', 'POST'])
@login_required
def BGI_Convert_Rate_Ajax():

    sql_query = text("""select symbol, max(time) as time, average from live1.daily_prices_temp as A
    where LENGTH(A.SYMBOL) = 6 and A.SYMBOL like "%USD%"
    group by A.SYMBOL""")

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]

    return_val=[]
    for r in result_data_clean:
        if not isinstance(r[0], str) or len(r) != 3 or r[2] == 0:
            continue
        #print(r)
        #print( r[0].find("USD"))
        if r[0].find("USD") == 3:
            r[0] = r[0][:3] # Want the front 3 letters
            r[2] = round(r[2], 4)
            return_val.append(r)
        elif r[0].find("USD") == 0:

            r[0] = r[0][3:] # Want the back 3 letters
            r[2] = 1/r[2]   # Need to reciprocal the amount
            r[2] = round(r[2], 4)
            return_val.append(r)

    result_dict = [dict(zip(result_col,d)) for d in return_val]

    return json.dumps(result_dict)


# Want to insert into table.
# From Flask.
@main_app.route('/Balance_equity_exclude', methods=['GET', 'POST'])
@login_required
def Exclude_Equity_Below_Credit():
    title = Markup("Exclude<br>Balance Below Credit")
    header = title
    description = Markup(
        "<b>To Exclude from the running tool of Balance_Below_Credit</b><br>Will add account into live1.balance_equity_exclude.<br>To allow client to trade on Credit")

    form = equity_Protect_Cut()
    print("Method: {}".format(request.method))
    print("validate_on_submit: {}".format(form.validate_on_submit()))
    form.validate_on_submit()
    if request.method == 'POST' and form.validate_on_submit():
        Live = form.Live.data  # Get the Data.
        Login = form.Login.data
        Equity_Limit = form.Equity_Limit.data

        sql_insert = """INSERT INTO  live1.`balance_equity_exclude` (`Live`, `Login`, `Equity_Limit`) VALUES
            ('{Live}','{Account}','{Equity}') ON DUPLICATE KEY UPDATE `Equity_Limit`=VALUES(`Equity_Limit`) """.format(Live=Live, Account=Login, Equity=Equity_Limit)
        sql_insert = sql_insert.replace("\t", "").replace("\n", "")

        print(sql_insert)
        db.engine.execute(text(sql_insert))  # Insert into DB
        flash("Live: {live}, Login: {login} Equity limit: {equity_limit} has been added to live1.`balance_equity_exclude`.".format(live=Live, login=Login, equity_limit=Equity_Limit))

    # flash("{symbol} {offset} updated in A Book offset.".format(symbol=symbol, offset=offset))
    # backgroud_Filename='css/Equity_cut.jpg', Table_name="Equity Protect Cut",  replace_words=Markup(["Today"])
    # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("General_Form.html",
                           title=title, header=header,
                           form=form, description=description)



# Want to show which clients got recently changed to read only.
# Due to Equity < Balance.
@main_app.route('/Futures/Scrape')
@login_required
def Scrape_futures():
    description = Markup("Scraping Futures.<br>" +
                         "Excel will be downloaded automatically.<br>"+
                         "Excel Data will be the same as those on the tables. ")

    # Template will take in an json dict and display a few tables, depending on how many is needed.
    return render_template("Webworker_1_table_Boderless_excel.html",
                           backgroud_Filename='css/Color-Pencil.jpg', \
                           title="Futures Scrape",
                           header="Scrape Futures",
                           excel_file_name = "Futures.xlsx",
                           ajax_url=url_for('main_app.Scrape_futures_ajax', _external=True),
                           description=description, replace_words=Markup(["Today"]))

@main_app.route('/Futures/Scrape_ajax',methods=['GET', 'POST'])
@login_required
def Scrape_futures_ajax():

    # Get all data from the web
    # Using the module "Scrape_Futures"
    return_val = Get_Current_Futures_Margin(db=db, sendemail=False)

    #return_val = [dict(zip(col, d)) for d in dict_of_df["US"]]
    #return_val = {'SG': [{"a":1, "b":2,"c":3}, {"a":2, "b":3,"c":4}],'UK': [{"d":10, "e":9,"f":8}, {"d":7, "e":6,"f":5}], 'US': [{"d":10, "e":9,"f":8}, {"d":7, "e":6,"f":5}]}
    #start_date = get_working_day_date(datetime.date.today(), weekdays_count=0)

    return json.dumps(return_val)



# Want to query SQL to pull and display all trades that might be the mismatched one.
def Mismatch_trades_mt4(symbol = [], hours=7, mins=16):


    live_server = [1,2,3,5]


    if len(symbol) > 0:
        symbol_list =  " AND (" + " OR ".join(["SYMBOL LIKE '%{}%'".format(s) for s in symbol]) + ")"
    else:
        symbol_list = ""

    # flexi time that we want to query the DB
    time_gmt_query = (datetime.datetime.now()-datetime.timedelta(hours=hours, minutes=mins)).strftime("%Y-%m-%d %H:%M:00")


    raw_query = """SELECT '{live}' as LIVE, LOGIN, TICKET, SYMBOL, CMD, VOLUME, OPEN_TIME, CLOSE_TIME, OPEN_PRICE, CLOSE_PRICE, `GROUP`, `COMMENT`  
    FROM live{live}.mt4_trades WHERE (OPEN_TIME >= '{time_query}' or CLOSE_TIME >= '{time_query}')
    and CMD < 6 and `GROUP` in (select * from live{live}.a_group) {symbol_list}"""
    raw_query = raw_query.replace("\n", "")

    mt4_query = " UNION ".join([raw_query.format(live=l, time_query = time_gmt_query, symbol_list=symbol_list) for l in live_server])

    sql_query = text(mt4_query)

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]

    return [result_col,result_data_clean]



# Want to query SQL to pull and display all trades that might be the mismatched one.
# symbol = ['USDJPY', 'EURUSD', 'USDSGD', 'XAUUSD']
def Mismatch_trades_bridge(symbol=[], hours=8, mins=16):

    if len(symbol) > 0: # If CFD, we want to replace . with %
        symbol_list =  " AND (" + " OR ".join(["SYMBOL LIKE '%{}%'".format(s.replace(".","%")) for s in symbol]) + ")"
    else:
        symbol_list = ""

    # GMT time that we can use to query Shiqi SQL
    time_start = datetime.datetime.now() - datetime.timedelta(hours=abs(hours), minutes=abs(mins))
    time_gmt_list = []

    date_now = datetime.datetime.now()
    while (time_start < date_now):
        time_gmt = time_start.strftime("%Y%m%d-%H") + "%"
        if time_gmt not in time_gmt_list:
            time_gmt_list.append(time_gmt)
        time_start = time_start +  datetime.timedelta(minutes=1)


    # SQ's DB tablenames.
    sq_tables = ["demo1", "demo2_new", "demo3_new"]

    time_str_condition = " OR ".join([" TRADETIME like '{}' ".format(t) for t in time_gmt_list])

    raw_query_bridge = """SELECT '{db}' as `DATABASE`, TRADETIME, REQUESTED_VOL, FILLED_VOL, SYMBOL, CMD, WAREHOUSE, LOGIN, `GROUP`, TICKET, TRADE_ID, ORDERNO FROM shiqi.{db}
    where WAREHOUSE = 'A' AND ({time_str_condition}) {symbol_list} """

    raw_query_bridge=raw_query_bridge.replace("\n", "")
    query_bridge_SQL = " UNION ".join([raw_query_bridge.format(db=db,time_str_condition=time_str_condition,symbol_list=symbol_list) for db in sq_tables])

    sql_query = text(query_bridge_SQL)

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]

    return [result_col, result_data_clean]


# Async Call to send email.
@async_fun
def async_send_email(To_recipients, cc_recipients, Subject, HTML_Text, Attachment_Name):
    Send_Email(To_recipients, cc_recipients, Subject, HTML_Text, Attachment_Name)


# Async Call to send telegram message.
@async_fun
def async_Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs, Parse_mode=""):

    if Parse_mode == "":
        Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs)
    else:
        Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs, Parse_mode = Parse_mode)



# Async update the runtime table for update.
@async_fun
def async_update_Runtime(app, Tool):

    #start = time.perf_counter()
    with app.app_context(): # Using current_app._get_current_object()
        # Want to update the runtime table to ensure that tool is running.
        sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
                     " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
                         Tool=Tool)
        raw_insert_result = db.engine.execute(sql_insert)
        # print("Updating Runtime for Tool: {}".format(Tool))
    #total_time = time.perf_counter() - start
    #print('Total Time taken for non-sync SQL insert: {}'.format(total_time))


#
# @main_app.route('/setinterval_test')
# @login_required
# def Aaron_test():
#     description = Markup("Testing Set Interval")
#     return render_template("Standard_Single_Table_Test.html", backgroud_Filename='css/Faded_car.jpg', Table_name="Testing", \
#                            title="Test", ajax_url=url_for('main_app.Aaron_test_ajax'),setinterval=5,
#                            description=description, replace_words=Markup(["Today"]))
#
#
#
# @main_app.route('/Aaron_test_ajax', methods=['GET', 'POST'])
# @login_required
# def Aaron_test_ajax():     # Return the Bloomberg dividend table in Json.
#
#     return_val=[{"Test":"Return: {}".format(time_now())}]
#     return json.dumps(return_val)


# To insert into SQL asynchronously.
# Header - To which database, table, and what columns
# values - Comes in a list, so we can decide how many times to insert it in.
# footer - On duplicate, what do we do?
# sql_max_insert - Optional. How many max do we want to insert at one time.

@async_fun
def async_sql_insert(app, header="", values = [" "], footer = "", sql_max_insert=500):

    with app.app_context():  # Using current_app._get_current_object()
        for i in range(math.ceil(len(values) / sql_max_insert)):
            # To construct the sql statement. header + values + footer.
            sql_trades_insert = header + " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]) + footer
            sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
            #print(sql_trades_insert)
            sql_trades_insert = text(sql_trades_insert)  # To make it to SQL friendly text.
            raw_insert_result = db.engine.execute(sql_trades_insert)
    return




# Function to update the SQL position from the CFH FIX.
# CFH_Position = {"EURUSD": 100000, "GBPUSD": 2300, ...}
def fix_position_sql_update(CFH_Position):

    # First, we want to update the position, as well as the updated time.
    fix_position_database = "aaron"
    fix_position_table = "cfh_live_position_fix"

    # Want to construct the statement for the insert into the  DB.table.
    # For the values that are non-zero
    fix_position_header = """INSERT INTO {fix_position_database}.{fix_position_table} (`Symbol`, `position`, `Updated_time`) VALUES """.format(
        fix_position_database=fix_position_database, fix_position_table=fix_position_table)
    fix_position_values = ["('{}', '{}', now()) ".format(k,d) for k,d in CFH_Position.items()]
    fix_position_footer = """ ON DUPLICATE KEY UPDATE position=VALUES(position), Updated_time=VALUES(Updated_time)"""

    # Async update SQL to save runtime
    async_sql_insert(app=current_app._get_current_object(),header=fix_position_header, values = fix_position_values, footer=fix_position_footer)

    if len(CFH_Position) == 0:
        CFH_Position[""] = ""


    # Want to Update to Zero, for those position that are not opened now.
    Update_to_zero = """UPDATE {fix_position_database}.{fix_position_table} set position = 0, Updated_time = now() where Symbol not in ({open_symbol})""".format(
        fix_position_database=fix_position_database, fix_position_table= fix_position_table,
        open_symbol = " , ".join(['"{}"'.format(k) for k in CFH_Position]))

    # Async update SQL. No header and footer as we will construct the whole statement here.
    async_sql_insert(app=current_app._get_current_object(),header="", values=[Update_to_zero], footer="")

    return



# Helper function to check if string is json
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except (ValueError):
    return False
  return True

# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_db_engine(sql_query):
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results


# Helper function to do a time check.
# Return "Update Slow<br> time" if it is more then 10 mins difference.
def time_difference_check(time_to_check):
    time_now = datetime.datetime.now()
    #time_now = datetime.datetime.now()  # Get the time now.
    if not isinstance(time_to_check, datetime.datetime):
        return "Error: Not datetime.datetime object"

    if abs((time_now - time_to_check).total_seconds()) > TIME_UPDATE_SLOW_MIN*60: # set the update to 10 mins.
        return time_to_check.strftime("<b>Update Slow</b><br>%Y-%m-%d<br>%H:%M:%S")
    else:
        return time_to_check.strftime("%Y-%m-%d<br>%H:%M:%S")

# Query SQL and return the Zip of the results to get a record.
def query_SQL_return_record(SQL_Query):
    raw_result = db.engine.execute(SQL_Query)
    result_data = raw_result.fetchall()
    result_col = raw_result.keys()
    collate = [dict(zip(result_col, a)) for a in result_data]
    return collate


# To get the User Data, for finance use. Used on download page.
def Live_MT4_Users(live):    # To upload the Files, or post which trades to delete on MT5
    raw_result = db.engine.execute('select login, `GROUP`, `NAME`, CURRENCY, REGDATE, `ENABLE`, `ENABLE_READONLY` from live{}.mt4_users '.format(live))
    # raw_result = db.engine.execute("select login, `NAME`, REGDATE, `GROUP`, CURRENCY from live3.mt4_users where login = 102")
    result_data = raw_result.fetchall()
    result_col = raw_result.keys()
    df_users = pd.DataFrame(data=result_data, columns=result_col)
    df_users["REGDATE"] = df_users["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    df_users["ENABLE"] = df_users["ENABLE"].apply(lambda x: "YES" if x==0 else "NO")
    df_users["ENABLE_READONLY"] = df_users["ENABLE_READONLY"].apply(lambda x: "YES" if x == 0 else "NO")
    return excel.make_response_from_array(list([result_col]) + list(df_users.values), 'csv', file_name="Live{}_Users.csv".format(live))


def create_table_fun(table_data):

    T = create_table()
    table = T(table_data, classes=["table", "table-striped", "table-bordered", "table-hover", "table-sm"])
    if (len(table_data) > 0) and isinstance(table_data[0], dict):
        for c in table_data[0]:
            if c != "\n":
                table.add_column(c, Col(c, th_html_attrs={"style": "background-color:# afcdff; word-wrap:break-word"}))
    return table

# Simple way of returning time string.
def time_now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")