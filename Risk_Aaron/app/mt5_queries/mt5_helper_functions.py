
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db

from flask_table import create_table, Col
from flask import url_for, current_app
import decimal
from Aaron_Lib import *
import pandas as pd
from unsync import unsync



# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_SQL_mt5_db_engine(sql_query):

    raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'mt5_live1'))

    #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
    #return raw_result

    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results