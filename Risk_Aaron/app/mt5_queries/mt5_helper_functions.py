
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db

from flask_table import create_table, Col
from flask import url_for, current_app
import decimal
from Aaron_Lib import *
from app.mt5_queries.mt5_sql_queries import *
import pandas as pd
from unsync import unsync



# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_mt5_db_engine(sql_query):

    raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'mt5_live1'))
    #raw_result = db5.engine.execute(text(sql_query))

    #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
    #return raw_result

    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results



def SQL_insert_MT5(header="", values = [" "], footer = "", sql_max_insert=500):

    for i in range(math.ceil(len(values) / sql_max_insert)):
        # To construct the sql statement. header + values + footer.
        sql_trades_insert = header + " , ".join(values[i * sql_max_insert:(i + 1) * sql_max_insert]) + footer
        sql_trades_insert = sql_trades_insert.replace("\t", "").replace("\n", "")
        #print(sql_trades_insert)
        sql_trades_insert = text(sql_trades_insert)  # To make it to SQL friendly text.
        raw_insert_result = db.session.execute(sql_trades_insert,  bind=db.get_engine(current_app, 'mt5_live1'))
        db.session.commit()
    return


def SQL_insert_MT5_statement(sql_insert):

    # To make it to SQL friendly text.
    raw_insert_result = db.session.execute( text(sql_insert),  bind=db.get_engine(current_app, 'mt5_live1'))
    #print(raw_insert_result.fetchall())
    #print(sql_insert)
    db.session.commit() # Since we are using session, we need to commit.
    return


# Want to get the MT5 PnL By Symbol from Yudi's Database.

def mt5_yesterday_symbol_pnl():
    sql_query = mt5_symbol_yesterday_pnl_query()
    result = Query_SQL_mt5_db_engine(sql_query)
    print(result)
    return result


