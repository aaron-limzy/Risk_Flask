
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db

from flask_table import create_table, Col

from Aaron_Lib import *

@async_fun
def async_sql_insert(app, header="", values = [" "], footer = "", sql_max_insert=500):

    print("Using async_sql_insert")

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

    print("Running Async Tool time update: {}".format(Tool))

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