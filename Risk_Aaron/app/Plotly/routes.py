from flask import Blueprint, render_template, Markup, url_for, request
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from app.Swaps.A_Utils import *
from sqlalchemy import text
from app.extensions import db, excel

import plotly
import plotly.graph_objs as go
import plotly.express as px

import pandas as pd
import numpy as np
import json

from app.Swaps.forms import UploadForm

from flask_table import create_table, Col

import requests

import pyexcel
from werkzeug.utils import secure_filename

analysis = Blueprint('analysis', __name__)

# @analysis.route('/Swaps/BGI_Swaps')
#
# def BGI_Swaps():
#     description = Markup("Swap values uploaded onto MT4/MT5. <br>\
#    Swaps would be charged on the roll over to the next day.<br> \
#     Three day swaps would be charged for FX on weds and CFDs on fri. ")
#
#     return render_template("Standard_Single_Table.html", backgroud_Filename='css/Faded_car.jpg', Table_name="BGI_Swaps", \
#                            title="BGISwaps", ajax_url=url_for("swaps.BGI_Swaps_ajax"),
#                            description=description, replace_words=Markup(["Today"]))
#
#



def create_plot():

    N = 40
    x = np.linspace(0, 1, N)
    y = np.random.randn(N)
    df = pd.DataFrame({'x': x, 'y': y}) # creating a sample dataframe

    data = [
        go.Scatter(
            x=df['x'], # assign x as the dataframe column 'x'
            y=df['y']
        )
    ]
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON



def get_Live1_Vol():


    query_bridge_SQL = """SELECT LOGIN, SYMBOL, CMD, VOLUME * 0.01 as VOLUME, OPEN_TIME, SWAPS + PROFIT AS REVENUE, live1.`mt4_trades`.`GROUP`
    FROM live1.`mt4_trades`,
    (SELECT `GROUP`,COUNTRY,CURRENCY,BOOK FROM live5.group_table WHERE LIVE = 'live1' AND COUNTRY = 'CN') AS X 
    WHERE mt4_trades.`GROUP`=X.`GROUP` AND LENGTH(mt4_trades.LOGIN) > 4 AND mt4_trades.CMD < 2 
    AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00'
    """

    sql_query = text(query_bridge_SQL)

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]

    df = pd.DataFrame(data=result_data, columns=result_col) # creating a dataframe

    df['SYMBOL'] = df['SYMBOL'].apply(lambda x: x[:6])
    df_sum = df.groupby('SYMBOL')['VOLUME'].sum().reset_index().sort_values('VOLUME', ascending=True)


    fig = px.bar(df, y=df_sum['SYMBOL'],x=df_sum['VOLUME'], orientation='h',
                 height=1000, width = 2000,
                 title='Open Position By volume')

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


    df_group_volume = df.groupby(['SYMBOL', 'GROUP'])['VOLUME'].sum().reset_index().sort_values('GROUP', ascending=True)
    df_heat_map_data = df_group_volume.pivot(index='SYMBOL', columns='GROUP')[['VOLUME']].fillna(0)

    z = [list([float(j) for j in df_heat_map_data.iloc[i].values]) for i in range(len(df_heat_map_data))]
    y = list(df_heat_map_data.index)
    x = list(df_heat_map_data.keys())

    fig = go.Figure(data=go.Heatmap(
        z = z, x = x, y=y,
        hoverongaps=False))
    fig.show()

    return graphJSON

@analysis.route('/analysis/plotly_index')
@login_required
def plotly_index():
    bar = get_Live1_Vol()
    return render_template('index_plotly.html', plot=bar)



