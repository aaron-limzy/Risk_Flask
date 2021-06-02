
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db
from Helper_Flask_Lib import check_session_live1_timing, profit_red_green
from flask_table import create_table, Col
from flask import url_for, current_app, session
import decimal
from Aaron_Lib import *
from app.mt5_queries.mt5_sql_queries import *
import pandas as pd
from unsync import unsync



# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_ticks_db_engine(sql_query):

    raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'risk_ticks'))
    #raw_result = db5.engine.execute(text(sql_query))

    #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
    #return raw_result

    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results
