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

# Want to write text of num in a shorter way.
def text_numbers(num):
    if abs(num) < 1000:
        return  "$ {:,.2f}".format(num)
    elif abs(num)<1000000:
        return  "$ {:,.2f}k".format(num/1000)
    else:
        return  "$ {:,.2f}M".format(num/1000000)


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



# TW Side SQL
def get_tw_df():

    query_bridge_SQL = """SELECT LOGIN, SYMBOL, CMD, SUM(VOLUME*0.01) as VOLUME, OPEN_TIME, SUM(SWAPS + PROFIT) AS REVENUE, live3.`mt4_trades`.`GROUP`
        FROM live3.`mt4_trades`
        WHERE mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00' and `GROUP` in (SELECT `GROUP` FROM live5.group_table 
                    WHERE LIVE = 'live3' AND COUNTRY = 'TW' and CURRENCY = 'USD')
        AND LENGTH(mt4_trades.LOGIN) > 4 AND mt4_trades.CMD < 2 
        GROUP BY LOGIN, SYMBOL, CMD       """
    return get_country_df(query_bridge_SQL)

# Get the Dataframe for CN
def get_cn_df():

    query_bridge_SQL = """SELECT LOGIN, SYMBOL, CMD, VOLUME * 0.01 as VOLUME, OPEN_TIME, SWAPS + PROFIT AS REVENUE, live1.`mt4_trades`.`GROUP`
    FROM live1.`mt4_trades`,
    (SELECT `GROUP`,COUNTRY,CURRENCY,BOOK FROM live5.group_table WHERE LIVE = 'live1' AND COUNTRY = 'CN') AS X 
    WHERE mt4_trades.`GROUP`=X.`GROUP` AND LENGTH(mt4_trades.LOGIN) > 4 AND mt4_trades.CMD < 2 
    AND mt4_trades.CLOSE_TIME = '1970-01-01 00:00:00'
    """
    return get_country_df(query_bridge_SQL)


def get_country_df(sql_statement):
    sql_query = text(sql_statement)

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    #result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]

    df = pd.DataFrame(data=result_data, columns=result_col) # creating a dataframe

    # Trying to get the net Volume
    df['NET_VOLUME'] = df.apply(lambda x: x['VOLUME'] if x['CMD'] == 0 else -1 * x['VOLUME'], axis=1)

    return df



@analysis.route('/analysis/cn_live_vol_ajax')
@login_required
# Gets the cn df, and uses it to plot the various charts.
def cn_live_vol_ajax():

    # start = datetime.datetime.now()
    # df = get_cn_df()
    # bar = plot_open_position_net(df, chart_title = "[CN] Open Position")
    # cn_pnl_bar = plot_open_position_revenue(df, chart_title="[CN] Open Position Revenue")
    # cn_heat_map = plot_volVSgroup_heat_map(df,chart_title="[CN] Net Position by Group")
    # print("Getting cn df and charts {} Seconds.".format((datetime.datetime.now()-start).total_seconds()))
    # vol_sum = round(sum(df['VOLUME']),2)
    # net_vol_sum = round(sum(df['NET_VOLUME']),2)
    # revenue_sum = round(sum(df['REVENUE']),2)
    # cn_summary = {'COUNTRY' : 'CN', 'VOLUME': vol_sum, "NET VOLUME": net_vol_sum, "REVENUE" : revenue_sum, 'TIME': Get_time_String()}
    # print(cn_summary)
    # return json.dumps([bar, cn_pnl_bar, cn_heat_map, cn_summary], cls=plotly.utils.PlotlyJSONEncoder)

    return get_country_charts(country="CN", df= get_cn_df())



@analysis.route('/analysis/tw_live_vol_ajax')
@login_required
# Gets the cn df, and uses it to plot the various charts.
def tw_live_vol_ajax():
    return get_country_charts(country="TW", df= get_tw_df())

# Generic get the country charts.
def get_country_charts(country, df):

    start = datetime.datetime.now()
    bar = plot_open_position_net(df, chart_title = "[{}] Open Position".format(country))
    pnl_bar = plot_open_position_revenue(df, chart_title="[{}] Open Position Revenue".format(country))
    heat_map = plot_volVSgroup_heat_map(df,chart_title="[{}] Net Position by Group".format(country))
    #print("Getting {} df and charts {} Seconds.".format(country,(datetime.datetime.now()-start).total_seconds()))
    vol_sum = '{:,.2f}'.format(round(sum(df['VOLUME']),2))
    revenue_sum = '{:,.2f}'.format(round(sum(df['REVENUE']),2))
    cn_summary = {'COUNTRY' : country, 'VOLUME': vol_sum, 'REVENUE' : revenue_sum, 'TIME': Get_time_String()}
    #print(cn_summary)
    return json.dumps([bar, pnl_bar, heat_map, cn_summary], cls=plotly.utils.PlotlyJSONEncoder)



def plot_open_position_net(df, chart_title):

    # Trying to get core symbol
    df['SYMBOL'] = df['SYMBOL'].apply(lambda x: x[:6])


    df['ABS_NET_VOLUME'] = abs(df['NET_VOLUME'])  # We want to look at the abs value. (Dosn't matter long or short)

    # Want to do the Group, to start having a consolidated df to plot.
    df_sum = df.groupby('SYMBOL')[['VOLUME', 'NET_VOLUME', 'ABS_NET_VOLUME']].sum().reset_index().sort_values(
        'ABS_NET_VOLUME', ascending=True)
    top_n_symbols = 15  # Want to see top how many?


    top_n_symbols = 15  # Want to see top how many?
    topn_symbol_list = list(df_sum.tail(top_n_symbols)['SYMBOL'].to_dict().values())
    df_sum_top_n = df_sum[df_sum['SYMBOL'].isin(topn_symbol_list)]

    vol_x = df_sum_top_n['VOLUME']
    net_vol_x = df_sum_top_n['NET_VOLUME']
    symbols = df_sum_top_n['SYMBOL']
    fig = go.Figure(data=[
        go.Bar(name="Total Volume", y=symbols, x=vol_x, orientation='h', text=vol_x, textposition='auto',
               cliponaxis=False, textfont=dict(size=14)),
        go.Bar(name="Net Volume", y=symbols, x=net_vol_x, orientation='h', text=net_vol_x, textposition='auto',
               cliponaxis=False, textfont=dict(size=14))
    ])

    # Change the bar mode
    fig.update_layout(barmode='group')
    fig.update_layout(
        autosize=False,
        width=800,
        height=800,
        margin=dict( pad=10),
        yaxis=dict(
            title_text="Symbols",
            titlefont=dict(size=20),
            ticks="outside", tickcolor='white', ticklen=15,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Volume",
            titlefont=dict(size=20),layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text='{} (Client Side)'.format(chart_title),
        titlefont=dict(size=28),
        title_x=0.5
    )
    # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #
    # return graphJSON
    return fig



def plot_open_position_revenue(df, chart_title):
    # Trying to get core symbol
    df['SYMBOL'] = df['SYMBOL'].apply(lambda x: x[:6])

    # Trying to get revenue on BGI Side
    df['REVENUE'] = -1 * df['REVENUE']  # Want to flip, for our View

    df_PnL = df.groupby('SYMBOL')[['REVENUE']].sum().reset_index().sort_values('REVENUE', ascending=True)
    top_n_symbols = 100  # Want to see top how many?
    topn_symbol_list = list(df_PnL.tail(top_n_symbols)['SYMBOL'].to_dict().values())
    df_sum_top_n = df_PnL[df_PnL['SYMBOL'].isin(topn_symbol_list)]

    pnl_x = df_sum_top_n['REVENUE'].round(2)
    pnl_text = pnl_x.apply(lambda x: text_numbers(x))  # for labelling
    pnl_color = pnl_x.apply(lambda x: "green" if x >= 0 else 'red')

    symbols = df_sum_top_n['SYMBOL']

    fig = go.Figure(data=[
        go.Bar(name="Total Volume", y=symbols, x=pnl_x, orientation='h', text=pnl_text, textposition='auto',
               cliponaxis=False,
               marker_color=pnl_color,
               textfont=dict(
                   size=14,
               ))
    ])

    # Change the bar mode
    fig.update_layout(barmode='group')

    # Figure Layout.
    fig.update_layout(
        autosize=False,
        width=800,
        height=800,
        yaxis=dict(
            title_text="Symbols",
            titlefont=dict(size=20),
            automargin=True,
            ticks="outside", tickcolor='white', ticklen=50,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Revenue",
            titlefont=dict(size=20),
            automargin=True,
            layer='below traces'
        ),
        xaxis_tickfont_size=15,
        title_text='{} (BGI Side)'.format(chart_title),
        titlefont=dict(size=28),
        title_x=0.5,
        margin=dict(
            pad=10)
    )

    fig.update_yaxes(automargin=True)
    #graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #return graphJSON
    return fig

@analysis.route('/analysis/plotly_index')
@login_required
def cn_index():

    return render_template('index_plotly.html', title="Float Country")



def plot_volVSgroup_heat_map(df, chart_title):

    df_group_volume = df.groupby(['SYMBOL', 'GROUP'])['NET_VOLUME'].sum().reset_index().sort_values('GROUP',
                                                                                                    ascending=True)
    df_heat_map_data = df_group_volume.pivot(index='SYMBOL', columns='GROUP')[['NET_VOLUME']]

    z = [list([float(j) for j in df_heat_map_data.iloc[i].values]) for i in range(len(df_heat_map_data))]
    y = list(df_heat_map_data.index)
    x = list(x[1] for x in df_heat_map_data.keys())

    fig = go.Figure(data=go.Heatmap(
        z=z, x=x, y=y,
        hoverongaps=False, colorscale='Viridis', reversescale=True))

    fig.update_layout(
        autosize=False,
        width=800,
        height=800,
        yaxis=dict(
            title_text="Symbols",
            titlefont=dict(size=20),
            automargin=True,
            ticks="outside", tickcolor='white', ticklen=50,
            layer='below traces'
        ),
        yaxis_tickfont_size=14,
        xaxis=dict(
            title_text="Net Volume",
            titlefont=dict(size=20),
            automargin=True,
            layer='below traces'
        ),
        xaxis_tickfont_size=10,
        title_text='{} (Client Side)'.format(chart_title),
        titlefont=dict(size=28),
        title_x=0.5,
        margin=dict(
            pad=10)
    )

    fig.update_yaxes(automargin=True)
    # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    # return graphJSON
    return fig


    # df_group_volume = df.groupby(['SYMBOL', 'GROUP'])['VOLUME'].sum().reset_index().sort_values('GROUP', ascending=True)
    # df_heat_map_data = df_group_volume.pivot(index='SYMBOL', columns='GROUP')[['VOLUME']].fillna(0)
    #
    # z = [list([float(j) for j in df_heat_map_data.iloc[i].values]) for i in range(len(df_heat_map_data))]
    # y = list(df_heat_map_data.index)
    # x = list(df_heat_map_data.keys())
    #
    # fig = go.Figure(data=go.Heatmap(
    #     z = z, x = x, y=y,
    #     hoverongaps=False))
    # fig.show()