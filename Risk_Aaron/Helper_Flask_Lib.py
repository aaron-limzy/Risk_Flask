
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db

from flask_table import create_table, Col
import decimal
from Aaron_Lib import *



if get_machine_ip_address() == '192.168.64.73': #Only On Server computer
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com", "Risk@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com", "risk@blackwellglobal.com", "cs@bgifx.com"]
    print("On Server 64.73")
else:
    EMAIL_LIST_ALERT = ["aaron.lim@blackwellglobal.com"]
    EMAIL_LIST_BGI = ["aaron.lim@blackwellglobal.com"]
    print("On Aaron's Computer")

EMAIL_AARON =  ["aaron.lim@blackwellglobal.com"]     # For test Groups.
EMAIL_LIST_RISKTW = ["aaron.lim@blackwellglobal.com", "fei.shao@blackwellglobal.com", "nicole.cheng@blackwellglobal.com"]



@async_fun
def async_sql_insert(app, header="", values = [" "], footer = "", sql_max_insert=500):

    #print("Using async_sql_insert")

    with app.app_context():  # Using current_app._get_current_object()
        for i in range(math.ceil(len(values) / sql_max_insert)):
            # To construct the sql statement. header + values + footer.
            sql_trades_insert = header + " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]) + footer
            sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
            #print(sql_trades_insert)
            sql_trades_insert = text(sql_trades_insert)  # To make it to SQL friendly text.
            raw_insert_result = db.engine.execute(sql_trades_insert)
    return

# Async update the runtime table for update.
@async_fun
def async_update_Runtime(app, Tool):

    #print("Running Async Tool time update: {}".format(Tool))

    # sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
    #              " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
    #                  Tool=Tool)
    # raw_insert_result = app.app_context().db.engine.execute(sql_insert)


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


def create_table_fun(table_data, additional_class=[]):

    T = create_table()
    table_class = additional_class + ["table", "table-striped", "table-bordered", "table-hover", "table-sm"]
    table = T(table_data, classes=table_class)
    if (len(table_data) > 0) and isinstance(table_data[0], dict):
        for c in table_data[0]:
            if c != "\n":
                table.add_column(c, Col(c, th_html_attrs={"style": "background-color:# afcdff; word-wrap:break-word"}))
    return table

# Simple way of returning time string.
def time_now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Query SQL and return the Zip of the results to get a record.
def query_SQL_return_record(SQL_Query):
    raw_result = db.engine.execute(SQL_Query)
    result_data = raw_result.fetchall()
    result_col = raw_result.keys()
    collate = [dict(zip(result_col, a)) for a in result_data]
    return collate


# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_db_engine(sql_query):
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results
