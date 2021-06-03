
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


import plotly
import plotly.graph_objs as go
import plotly.express as px


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


# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def Query_Symbol_Markup_db_engine(sql_query):

    raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'mt5_futures'))
    #raw_result = db5.engine.execute(text(sql_query))

    #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
    #return raw_result

    result_data = raw_result.fetchall()
    result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
    result_col = raw_result.keys()
    zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
    return zip_results




@unsync
# Input: sql_query.
# Return a Dict, using Zip for the results and the col names.
def unsync_Query_SQL_ticks_db_engine(app_unsync, sql_query, date_to_str=True):

    results = [{}]
    with app_unsync.app_context():  # Need to use original app as this is in the thread

        raw_result = db.session.execute(text(sql_query), bind=db.get_engine(app_unsync, 'risk_ticks'))
        result_data = raw_result.fetchall()

        # If we need to change date to string
        if date_to_str:
            result_data = [["{}".format(a) if isinstance(a, datetime.datetime) else a for a in d] for d in
                               result_data]

        result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d] for d in
                               result_data]  # correct The decimal.Decimal class to float.

        result_col = raw_result.keys()
        results = [dict(zip(result_col, d)) for d in result_data_decimal]

        #db.session.close()

    return results

def plot_symbol_Spread(df, chart_title):


    # To ensure that the columns are all in.
    if not all(x in df for x in ["DATE_TIME", "Spread_Digits", "Symbol"]):
        print("Column missing from df for plotting symbol spread.")
        print(df)
        return []

    fig = px.line(df, x="DATE_TIME", y="Spread_Digits", color='Symbol')

    # Figure Layout.
    fig.update_layout(
        autosize=True,
        height=1000,
        yaxis=dict(
            title_text="Spread (Points)",
            titlefont=dict(size=20),
            automargin=True,
            ticks="outside", tickcolor='white', ticklen=50,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Date",
            titlefont=dict(size=20),
            automargin=True,
            layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text=chart_title,
        titlefont=dict(size=20),
        title_x=0.5,
        margin=dict(
            pad=10)
    )

    # hide weekends
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"])  # hide weekends
        ]
    )

    fig.update_yaxes(automargin=True)
    return fig
