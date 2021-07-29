# # from PIL import Image
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#from json2html import *
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
# import ctypes
import datetime
import json
# #import MySQLdb  # Need to install with the whl file....
import os
import random
import re
import smtplib
import subprocess
import ctypes
import time
import telegram #pip install python-telegram-bot==12.0.0b1 --upgradepymysql

import pymysql
import socket
import math
import pymysql
from sqlalchemy import create_engine, text

from io import StringIO


# API Guide to Telegram.
# https://core.telegram.org/bots/api

SQL_IP = "192.168.64.73"
SQL_User = "mt4"
SQL_Password = "1qaz2wsx"
SQL_Database = "aaron"
SQL_Table = "bloomberg_dividend"

Email_Header = "<style>p.a {font-family:Calibri;panose-1:2 15 5 2 2 2 4 3 2 4;font-size: 14.5px;}</style><p Class='a'>"
Email_Footer = "</p>"

# Telegram
ID = "486797751"


AARON_BOT = "736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0"		# For Mismatch and LP Margin

TELE_ID_MTLP_MISMATCH = "736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0"		# For Mismatch and LP Margin
TELE_ID_USDTWF_MISMATCH = "776609726:AAHVrhEffiJ4yWTn1nw0ZBcYttkyY0tuN0s"        # For USDTWF
TELE_ID_MONITOR = "1055969880:AAHcXIDWlQqrFGU319wYoldv9FJuu4srx_E"      # For BGI Monitor
TELE_CLIENT_ID = ["486797751"]        # Aaron's Telegram ID.


# # token = "736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0"		# For Mismatch and LP Margin
# # token = "776609726:AAHVrhEffiJ4yWTn1nw0ZBcYttkyY0tuN0s"        # For USDTWF



def Send_Email(To_recipients, cc_recipients, Subject, HTML_Text, Attachment_Name, virtual_file={}):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()  # Hello to SMTP server
    server.starttls()  # Need to start the TLS connection
    server.login("aaron.riskbgi@gmail.com", "ReportReport")  # Login with credientials

    me = "aaron.riskbgi@gmail.com"
    you = To_recipients
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart()
    msg = MIMEMultipart('alternative')
    # Bcc_recipients = ["aaron.lim@blackwellglobal.com"]

    msg['Subject'] = Subject
    msg['From'] = me
    msg['To'] = ",".join(To_recipients)
    msg['cc'] = ",".join(cc_recipients)
    # msg['Bcc'] = ",".join(Bcc_recipients)  # Have to add this function at a later date...

    for i in range(len(Attachment_Name)):
        Buffer = Attachment_Name[i].split('/')
        filename = Buffer[len(Buffer) - 1]
        attachment = open(Attachment_Name[i], "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        # Only Want to add the part of the name where it is the excel name.
        part.add_header('Content-Disposition',
                        "attachment; filename= " + filename.split("\\")[len(filename.split("\\")) - 1])
        msg.attach(part)

    for k,d in virtual_file.items():
        m_1 = MIMEBase('application', "octet-stream")
        m_1.set_payload(d.getvalue())
        encoders.encode_base64(m_1)
        m_1.add_header('Content-Disposition',
                       'attachment',
                       filename='{}.txt'.format(k))
        msg.attach(m_1)

    p = MIMEText(HTML_Text, 'html')
    msg.attach(p)
    server.sendmail(me, To_recipients + cc_recipients, msg.as_string())
    # server.sendmail(me, To_recipients + Bcc_recipients + cc_recipients, msg.as_string())
    return

def create_email_virtual_file(txt)

    f = StringIO()
    f.write(txt)
    f.seek(0)
    return f


def Get_time_String(datetime_format = None):
    if datetime_format == None: # If there is no input
        datetime_format = datetime.datetime.now()

    return datetime_format.strftime("%Y-%b-%d %H:%M:%S")

def Get_SQL_Timestring(datetime_format=None):
    if datetime_format == None:  # If there is no input
        datetime_format = datetime.datetime.now()
    return datetime_format.strftime("%Y-%m-%d %H:%M:%S")

    # return str(now.year) + "-" + str(now.month) + "-" + str(now.day) + "_" + str(now.hour) + "-" + str(
    #    now.minute) + "-" + str(now.second)

def readable_format(val):
    if isinstance(val, float):  #Return comma seperated, 2dp string
        return "{:,.2f}".format(val)
    if  isinstance(val, datetime.datetime):
        return Get_time_String(val)
    return "{}".format(val)


def Get_time_String_Simple():
    now = datetime.datetime.now()
    return now.strftime("%Y-%b-%d %H:00")
#
#
# def Call_Alert():
#     ctypes.windll.user32.MessageBoxW(0, "Full PnL Picture Missing", "Picture Missing", 1)
#

# Array to HTML Table.
# ['table', 'table-striped', 'table-hover', 'table-bordered', 'table-light', 'table-sm']
def Array_To_HTML_Table(Table_Header, Table_Data,  Highlight_words = [], Table_Class = []):
    # HTML_Table_Header = "<table border = \"1\" cellpadding = \"4\" bordercolor = \"Black\" style=\"text-align:center;\"  >"
    #HTML_Table_Header = "<table border = \"2\" cellpadding = \"4\" bordercolor = \"Black\" style=\"text-align:center; \"  >"


    if len(Table_Class) == 0:  # Will set a default table class if needed, else, set to the classes needed
        HTML_Table_Header = "<table border = \"2\" cellpadding = \"4\" bordercolor = \"Black\" style=\"text-align:center; \"  >"
    else:
        HTML_Table_Header = '<table class="{table_class}">'.format(table_class=" ".join(Table_Class))


    HTML_Table_Code = HTML_Table_Header
    Header_Count = len(Table_Header)

    # Add the Table Header.
    HTML_Table_Code = HTML_Table_Code + " <tr bgcolor=\"#BBC7FF\"> "
    # HTML_Table_Code = HTML_Table_Code + " <tr bgcolor=\"#0052cc\">  "
    for i in range(Header_Count):
        # HTML_Table_Code = HTML_Table_Code + " <th><FONT COLOR=white>" + Table_Header[i] +"</FONT></th>"
        HTML_Table_Code = HTML_Table_Code + " <th>" + Table_Header[i] + "</th>"
    HTML_Table_Code = HTML_Table_Code + " </tr> "

    # Add the Table Data.
    for i in range(len(Table_Data)):
        HTML_Table_Code = HTML_Table_Code + " <tr> "
        for j in range(len(Table_Data[i])):
            if any([Table_Data[i][j].find(w) >= 0 for w in Highlight_words]):   # If there are any words to be highlighted.
                HTML_Table_Code = HTML_Table_Code + " <td bgcolor=#ff9999>" + str(Table_Data[i][j]) + "</td>"
            else:
                HTML_Table_Code = HTML_Table_Code + " <td>" + str(Table_Data[i][j]) + "</td>"


        HTML_Table_Code = HTML_Table_Code + " </tr> "
    HTML_Table_Code = HTML_Table_Code + "</table>"
    return HTML_Table_Code



# A list of Dictionary to HTML Table.
# Word List to turn the cell red should it be needed.
# [{},{},{}]
def List_of_Dict_To_Horizontal_HTML_Table(Data_Dict_List, Word_List=[]):

    if len(Data_Dict_List) == 0:    # Nothing here.
        return ""

    Table_Header = list(Data_Dict_List[0].keys())
    Table_Data = [list(d.values()) for d in Data_Dict_List]

    # HTML_Table_Header = "<table border = \"1\" cellpadding = \"4\" bordercolor = \"Black\" style=\"text-align:center;\"  >"
    HTML_Table_Header = "<table border = \"2\" cellpadding = \"4\" bordercolor = \"Black\" style=\"text-align:center;\"  >"
    HTML_Table_Code = HTML_Table_Header

    # Add the Table Header.
    # Header Label.
    HTML_Table_Code +=  " <tr bgcolor=\"#BBC7FF\"> " + " ".join(["<th>{}</th>".format(th) for th in Table_Header])  + "</tr>"


    # Add the Table Data.
    for i in range(len(Table_Data)):
        HTML_Table_Code = HTML_Table_Code + " <tr> "
        for j in range(len(Table_Data[i])):
            if isinstance(Table_Data[i][j], dict):  # Calling recursively
                HTML_Table_Code += " <td>" + Dict_To_Vertical_HTML_Table(Table_Data[i][j]) + "</td>"
            else:
                # Want to turn the cell red if there are any alert words found.
                HTML_Table_Code += "<td bgcolor=\"#ff8080\">" if any([Table_Data[i][j].find(a) >= 0 for a in Word_List]) else "<td>"
                HTML_Table_Code +=  str(Table_Data[i][j]) + "</td>"


        HTML_Table_Code = HTML_Table_Code + " </tr> "



    HTML_Table_Code = HTML_Table_Code + "</table>"
    return HTML_Table_Code



# Dictionary to vertical HTML Table.
# {}
def Dict_To_Vertical_HTML_Table(Data_Dict_List):

    if len(Data_Dict_List) == 0:    # Nothing here.
        return ""

    # HTML_Table_Header = "<table border = \"1\" cellpadding = \"4\" bordercolor = \"Black\" style=\"text-align:center;\"  >"
    HTML_Table_Header = "<table border = \"1\" cellpadding = \"3\" bordercolor = \"Grey\" style=\"text-align:center; border-collapse: collapse;\"  >"
    HTML_Table_Code = HTML_Table_Header

    for k,d in Data_Dict_List.items():
        HTML_Table_Code += " <tr> <th bgcolor=\"#ffcc80\">{k}</th><td>{d}</td></tr>".format(k=k, d=d)

    HTML_Table_Code = HTML_Table_Code + "</table>"
    return HTML_Table_Code



# # Query SQL and return the results in an array.
# # [Results Data, Column Data]
# # Uses pymysql instead
def Query_SQL(SQL_Query):

    connection = pymysql.connect(host=SQL_IP, user=SQL_User, passwd=SQL_Password, db=SQL_Database)
    connection.autocommit = True        # Set it to always commit.
    Cursor = connection.cursor()
    #SQL_Query = "Select * from aaron.bloomberg_dividend WHERE `date` >= NOW() AND WEEKDAY(`date`) BETWEEN 0 AND 4 ORDER BY date "
    #SQL_Query = "Select * from aaron.bloomberg_dividend WHERE `date` >= NOW() AND WEEKDAY(`date`) BETWEEN 0 AND 4 and `date`='2018-10-12'  ORDER BY date"
    Cursor.execute(SQL_Query)
    results = Cursor.fetchall()     # Is in Tuple
    Column_Details = Cursor.description # To get the column details, with length and etc...
    Cursor.close() # Close the Cursor.
    connection.close()

    return [tuple_to_array(results),tuple_to_array(Column_Details)]


# Write a Tuple to be an array
def tuple_to_array(Tuple_t):
    Return_array = []
    for t in Tuple_t:
        Return_Array_Buffer = []
        for tt in t:
            Return_Array_Buffer.append(tt)
        Return_array.append(Return_Array_Buffer)
    return Return_array


# Check if element is a float string. if it is, return true, else, false
def Check_Float(element):
    try:
        float(element)
        return True
    except ValueError:
        #print("Not a float")
        return False


# To run C Programs
def Run_C_Prog(Path, cwd=None):

    path = Path  # Need as Buffer, to append the added cwd if needed
    #print(os.getcwd())

    if cwd != None:  # We need to append the full (relative) path
        path = cwd + "\\" + path

    #print("Run C Prog: {}".format(path))

    p = subprocess.Popen(path, stdout=subprocess.PIPE, cwd=cwd)
    (output, err) = p.communicate()  # Want to capture the COUT
    #print("output: {},  err: {}".format(output, err))
    C_Return_Val = ctypes.c_int32(p.returncode).value   # Convert to -ve/+ve.
    return (C_Return_Val, output, err)


# #Post_To_Telegram("708830467:AAHq9GVujNqPhvAKiXhMhqW_Qsl9ObdYiY4", "Test: Hello World.", ["486797751"])
def Post_To_Telegram(Bot_token, text_to_tele, Chat_IDs, Parse_mode=telegram.ParseMode.MARKDOWN):
    #URL = "https://api.telegram.org/bot" + Bot_token + "/"
    #https://api.telegram.org/bot708830467:AAHq9GVujNqPhvAKiXhMhqW_Qsl9ObdYiY4/getUpdates
    bot = telegram.Bot(token=Bot_token)
    for c_id in Chat_IDs:
        bot.sendMessage(chat_id=c_id, text=text_to_tele, parse_mode=Parse_mode)
#Chat_IDs=[486797751]
#Bot_token = "708830467:AAHq9GVujNqPhvAKiXhMhqW_Qsl9ObdYiY4"


# Want to either count forward or backwards, the number of weekdays.
# weekdaylist: 0 = Monday, 6 = Sunday
def get_working_day_date(start_date, weekdays_count, weekdaylist = [0,1,2,3,4]):
    increment_decrement_val = 1 if weekdays_count >=0 else -1
    week_day_count = abs(weekdays_count)
    return_date = start_date
    backtrace_days = 0
    weekday_count = 0   # how many weekdays have we looped thru
    while weekday_count < week_day_count: # How many weekdays do we want to go back by?
        backtrace_days += increment_decrement_val   # Either + 1 or -1
        return_date = start_date + datetime.timedelta(days=backtrace_days)
        if return_date.weekday() in weekdaylist: # We will count it if weekdays.
            weekday_count += 1

    return return_date

# Check if the string float.
def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False



# Helper function to check if string is json
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except (ValueError):
    return False
  return True

# Get machine IP.
def get_machine_ip_address():

    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    #print("Your Computer Name is:" + hostname)
    #print("Your Computer IP Address is:" + IPAddr)
    return(IPAddr)


# CFH Fix works from UTC Sunday 1730 - Friday 2215
# GMT = UTC timing
# We want to allow for Sunday 1735 - Friday 2210 Connection.
# Giving 5 mins buffer.
def cfh_fix_timing():
    datetime_now = datetime.datetime.utcnow()
    weekd = datetime_now.weekday()  # Monday = 0

    # # For Testing
    # if weekd == 3 and datetime_now.time() > datetime.time(8, 7, 0):
    #     return False

    if weekd >= 4:
        if weekd == 4 and datetime_now.time() > datetime.time(22,10,0):  # Friday
            return False
        if weekd == 5:  # Sat
            return False
        if weekd == 6 and datetime_now.time() < datetime.time(17,35,0):   # Sunday
            return False
    return True


# Want to attempt to find the CFD's core symbol.
# no need the postfix
def cfd_core_symbol(str_val):

    if str_val.find(".") != 0:
        return str_val
    cfd_symbol = ['.A50', '.AUS200', '.DE30', '.ES35', '.F40', '.HK50', '.JP225', '.STOXX50', '.UK100', '.UKOil', '.US100', '.US30',
     '.US500', '.USOil']
    for c in cfd_symbol:
        if str_val.lower().find(c.lower()) == 0:
            return c
    return str_val



# Will take a pandas dataframe, and transforms it into an array of dicts.
def pd_dataframe_to_dict(df_buff):

    df_records = df_buff.to_records(index=False)
    dataframe_col = list(df_buff.columns)
    df_records = [list(a) for a in df_records]

    return [dict(zip(dataframe_col,d)) for d in df_records]


# To insert SQL to another DB
def SQL_Insert_Host(SQL_IP, SQL_User, SQL_Password, SQL_Database, SQL_Query_Str):

    connection = pymysql.connect(host=SQL_IP, user=SQL_User, passwd=SQL_Password, db=SQL_Database)
    connection.autocommit = True  # Set it to always commit.
    Cursor = connection.cursor()
    Cursor.execute(SQL_Query_Str)
    connection.commit()



def Query_SQL_Host(SQL_Query, SQL_ip, SQL_user, SQL_password, SQL_database):

    connection = pymysql.connect(host=SQL_ip, user=SQL_user, passwd=SQL_password, db=SQL_database)
    connection.autocommit = True        # Set it to always commit.
    Cursor = connection.cursor()
    Cursor.execute(SQL_Query)
    results = Cursor.fetchall()     # Is in Tuple
    Column_Details = Cursor.description # To get the column details, with length and etc...
    Cursor.close() # Close the Cursor.
    connection.close()

    return [tuple_to_array(results),tuple_to_array(Column_Details)]



# Want to use the same code as Flask.
# Will use sqlalchemy instead of py-sql
# db = init_SQLALCHEMY()
def init_SQLALCHEMY():
    #TODO: Link this with Flask's config. Else, there  might be trouble when moving SQL Bases.
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/aaron'
    return create_engine(SQLALCHEMY_DATABASE_URI)

# db for MT5
# db5 = init_SQLALCHEMY_mt5()
def init_SQLALCHEMY_mt5():
    #TODO: Link this with Flask's config. Else, there  might be trouble when moving SQL Bases.
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://risk:1qaz2wsx@119.81.149.213/aaron'
    return create_engine(SQLALCHEMY_DATABASE_URI)


#Want to take in values and header of SQL
# Return an array of SQL statement ready for insert.
def sql_multiple_insert(tablename="", column = "" ,values = [" "], footer = "", sql_max_insert=500):

    sql_statement = []
    for i in range(math.ceil(len(values) / sql_max_insert)):
        sql_trades_insert = ""
        # To construct the sql statement. header + values + footer.
        sql_trades_insert = """INSERT INTO {tablename} {column} VALUES {sql_data} {footer} """.format(tablename=tablename,
                column=column, sql_data= " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]),  footer=footer)

        #sql_trades_insert = header +  + " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]) + footer
        sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
    sql_statement.append(sql_trades_insert)
    return sql_statement


# a = """<b>Account Monitoring</b>
#
# <u><b>Open Trade/s</b></u>
# <code>L|LOGIN  |C| LOT | SYMBOL  |OPEN PRICE</code>
# 2|2040    |B|0.01| XAUUSD! |1736.36|
# 2|2040    |S|0.01| XAUUSD! |1735.1 |
#
# <b>Closed Trade/s</b>
# <pre>L|LOGIN|C| LOT | SYMBOL  |CLOSE PRICE|PROFIT</pre>
# 2|<code>2040    </code>|B|0.01| XAUUSD! |1738.01|2.41
# """
# Post_To_Telegram("736426328:AAH90fQZfcovGB8iP617yOslnql5dFyu-M0", a,  ["486797751"], Parse_mode=telegram.ParseMode.HTML)