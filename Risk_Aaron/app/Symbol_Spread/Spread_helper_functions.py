
import math
from sqlalchemy import text
from app.decorators import async_fun
from app.extensions import db
from Helper_Flask_Lib import check_session_live1_timing, profit_red_green
from flask_table import create_table, Col
from flask import url_for, current_app, session
import decimal
from Aaron_Lib import *
from Helper_Flask_Lib import *
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


# # Input: sql_query.
# # Return a Dict, using Zip for the results and the col names.
# def Query_Symbol_Markup_db_engine(sql_query):
#
#     raw_result = db.session.execute(text(sql_query), bind=db.get_engine(current_app, 'mt5_futures'))
#     #raw_result = db5.engine.execute(text(sql_query))
#
#     #db.session.execute(sql_query, bind=db.get_engine(current_app, 'mt5_live1'))
#     #return raw_result
#
#     result_data = raw_result.fetchall()
#     result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d ] for d in result_data]    # correct The decimal.Decimal class to float.
#     result_col = raw_result.keys()
#     zip_results = [dict(zip(result_col,d)) for d in result_data_decimal]
#     return zip_results




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

def plot_symbol_Spread(df, chart_title, x_axis):


    # To ensure that the columns are all in.
    if not all(x in df for x in [x_axis, "SPREAD", "SYMBOL"]):
        print("Column missing from df for plotting symbol spread.")
        print(df)
        return []

    fig = px.line(df, x=x_axis, y="SPREAD", color='SYMBOL', template='ggplot2')

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
            title_text="{}".format(x_axis),
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

    # If there is time, we don't want to hide any column.
    if x_axis.lower().find("time") < 0:
        # hide weekends
        fig.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"])  # hide weekends
            ]
        )

    fig.update_yaxes(automargin=True)
    return fig

# Create all the timings, by hour
# So that the Graph lines won't be drawn.
def create_all_timing(df):
    # Make the time for all 24 hours. So that there will be null.
    date_all_from_df = [d for d in df["DATE"].unique().tolist()]
    # We want to compute the weekends as well.
    date_all = [min(date_all_from_df) + datetime.timedelta(days=i) for i in
                range((max(date_all_from_df) - min(date_all_from_df)).days)]

    date_time_all = [datetime.datetime.combine(d, datetime.time.min) + datetime.timedelta(hours=h) for d in date_all for
                     h in range(24)]
    df_time_all = pd.DataFrame(date_time_all, columns=["DATE_TIME"])
    return df_time_all

def plot_symbol_Spread_individual(df, chart_title, x_axis):


    # To ensure that the columns are all in.
    if not all(x in df for x in [x_axis]):
        print("Column missing from df for plotting symbol spread.")
        print(df)
        return []

    list_of_charts = []

    # We want the list of columns to plog
    list_of_col = [c for c in df.columns if any(c.find(x) >= 0 for x in ["MAX", "AVG", "MIN"])]

    for c in list_of_col:
        if c in df:
            list_of_charts.append(go.Scatter(
            name='{c} Spread'.format(c=c),
            x=df['DATE_TIME'],
            y=df[c],
            mode='lines',
            showlegend=True,
            connectgaps=False
        ))

    # Create all the trace`
    fig = go.Figure(list_of_charts)

    # fig = go.Figure([
    #     go.Scatter(
    #         name='Max Spread',
    #         x=df['DATE_TIME'],
    #         y=df['MAX'],
    #         mode='lines',
    #         marker=dict(color='red'),
    #         showlegend=True,
    #         connectgaps=False
    #     ),
    #     go.Scatter(
    #         name='Average',
    #         x=df['DATE_TIME'],
    #         y=df['AVG'],
    #         marker=dict(color="black", size=2),
    #         line=dict(width=1),
    #         mode='lines',
    #         showlegend=True,
    #         connectgaps=False
    #     ),
    #     go.Scatter(
    #         name='Min Spread',
    #         x=df['DATE_TIME'],
    #         y=df['MIN'],
    #         mode='lines',
    #         marker=dict(color="blue"),
    #         line=dict(width=1),
    #         showlegend=True,
    #         connectgaps=False
    #     ),
    #
    #     #     go.Scatter(
    #     #         name='Upper Bound',
    #     #         x=df['Time'],
    #     #         y=df['Velocity']+df['SEM'],
    #     #         mode='lines',
    #     #         marker=dict(color="#444"),
    #     #         line=dict(width=1),
    #     #         showlegend=False
    #     #     ),
    #     #     go.Scatter(
    #     #         name='Lower Bound',
    #     #         x=df['Time'],
    #     #         y=df['Velocity']-df['SEM'],
    #     #         marker=dict(color="#444"),
    #     #         line=dict(width=1),
    #     #         mode='lines',
    #     #         fillcolor='rgba(68, 68, 68, 0.3)',
    #     #         fill='tonexty',
    #     #         showlegend=False
    #     #     )
    # ])


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
            title_text="Live 2 Server Time",
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

    return fig


# Query SQL and return the Zip of the results to get a record.
# Ment to be called by a function that has @unsync declared
def unsync_query_SQL_ticks_return_record(SQL_Query, app_unsync, date_to_str=True):

    results = [{}]
    with app_unsync.app_context():  # Need to use original app as this is in the thread

        raw_result = db.session.execute(text(SQL_Query), bind=db.get_engine(app_unsync, 'risk_ticks'))
        result_data = raw_result.fetchall()
       # If we need to change date to string
        if date_to_str:
            result_data = [["{}".format(a) if isinstance(a, datetime.datetime) else a for a in d] for d in
                               result_data]

        result_data_decimal = [[float(a) if isinstance(a, decimal.Decimal) else a for a in d] for d in
                               result_data]  # correct The decimal.Decimal class to float.

        result_col = raw_result.keys()
        results = [dict(zip(result_col, d)) for d in result_data_decimal]

    return results

@unsync
# Get other broker's ticks. Only can get 1 symbol at once.
# As it queries the symbol table directly
def get_other_broker_ticks_unsync(sql_database, symbol, date_start, symbol_digits, app_unsync, pre_fix=None):

    # Preparing to get Global_prime's Ticks
    #sql_database = "global_prime"
    sql_query = """SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = "{sql_database}" and TABLE_NAME rlike '{Symbol}' """.format(
        sql_database=sql_database, Symbol=symbol)
    # Get it from the Ticks DB
    res_broker = unsync_query_SQL_ticks_return_record(SQL_Query=sql_query, \
                                    app_unsync=app_unsync, \
                                                       date_to_str=False)
    pre_fix_1 = pre_fix if pre_fix != None else sql_database


    broker_table_name = res_broker[0]["TABLE_NAME"] if len(res_broker) > 0 and "TABLE_NAME" in res_broker[0] else ""
    print(f"broker_table_name: {broker_table_name}")


    sql_query = """SELECT DATE(DATE_TIME) as `DATE`, HOUR(DATE_TIME) as `HOUR`, (ASK-BID) as AVG, MAX(ASK-BID) as MAX, MIN(ASK-BID) as MIN          
                FROM {sql_database}.`{sql_table}` 
                WHERE DATE_TIME >= '{date}'
                GROUP BY LEFT(DATE_TIME, 13)""".format(date=date_start, sql_table=broker_table_name,
                                                       sql_database=sql_database).replace("\n", " ")

    res_broker_ticks = unsync_query_SQL_ticks_return_record(SQL_Query=sql_query, \
                                           app_unsync=app_unsync, \
                                                       date_to_str=False)

    df_Symbol_Ticks_Broker = pd.DataFrame(data=res_broker_ticks)
    # Digit Correction
    df_Symbol_Ticks_Broker["AVG"] = df_Symbol_Ticks_Broker["AVG"] * (10**symbol_digits)
    df_Symbol_Ticks_Broker["MAX"] = df_Symbol_Ticks_Broker["MAX"] * (10**symbol_digits)
    df_Symbol_Ticks_Broker["MIN"] = df_Symbol_Ticks_Broker["MIN"] * (10**symbol_digits)

    # Concat the Date with the Hour
    # Global Prime has is 1 hour behind Live 2 ticks.
    df_Symbol_Ticks_Broker["DATE_TIME"] = df_Symbol_Ticks_Broker.apply(
        lambda x: x["DATE"] + pd.DateOffset(hours=x["HOUR"] - 1), axis=1)

    df_Symbol_Ticks_Broker.rename(columns={"AVG": "{} AVG".format(pre_fix_1),
                                           "MAX": "{} MAX".format(pre_fix_1),
                                           "MIN": "{} MIN".format(pre_fix_1),
                                           }, inplace=True)

    # Want to drop the columns that are not needed.
    for c in ["HOUR", "DATE"]:
        if c in df_Symbol_Ticks_Broker:
            df_Symbol_Ticks_Broker.drop(c, axis=1)


    return df_Symbol_Ticks_Broker


# To rename the column
# So that we can get 1 particular column
# and we can pivot on it later.
def get_df_slice(df, col):
    df_to_pivot = df[["DATE_TIME", col]]
    df_to_pivot.columns = ["DATE_TIME", "VALUE"]
    df_to_pivot["TYPE"] = col
    return df_to_pivot

# To get the URL for the Live/Login to run client analysis
# For telegram. Will replace localhost with the external IP
# TODO: Need to put external IP into a seperate File for easy change.
def Symbol_spread_url_External(Symbol):

    url = url_for('Spread.individual_symbol_spread',symbol=Symbol.upper(), _external=True)
    return '<a href="{url}">{Symbol}</a>'.format(url=url, Symbol=Symbol.upper())
