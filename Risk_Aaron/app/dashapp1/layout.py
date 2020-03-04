import dash_core_components as dcc
import dash_html_components as html


# from iexfinance.stocks import get_historical_data
# import datetime
# from dateutil.relativedelta import relativedelta
# import plotly.graph_objs as go
#
# start = datetime.datetime.today() - relativedelta(years=1)
# end = datetime.datetime.today()
#
# df = get_historical_data("GE", start=start, end=end, token='pk_0a10f76c4b5c45f9b57d4afc2efa1801', output_format="pandas")
# trace_close = go.Scatter(x=list(df.index), y = list(df.close), name="Close", line=dict(color="#f44242"))
# data = [trace_close]
# layout=dict(title="stock chart", showledgend=False)
# fig=dict(data=data, layout=layout)
#
#
# layout = html.Div([
#     html.Div(html.H1(children='Stock Tickers')),
#     html.Div(
#         dcc.Graph(id='my-graph', figure=fig)
#     )
# ], style={'width': '500'})

layout = html.Div([ html.Div(html.H1(children='Stock Tickers'))])