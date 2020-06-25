from flask_table import Table, Col, LinkCol, ButtonCol
from flask import url_for, redirect, request



# # Variable name has to be the Dict name.
# # Has to provide a list of Dicts.
# class Client_Trade_Table(Table):
#     TICKET = Col('TICKET')
#     SYMBOL = Col('SYMBOL')
#     LOTS = Col('LOTS')
#     CMD = Col('CMD')
#     OPEN_TIME = Col('OPEN_TIME')
#     CLOSE_TIME = Col('CLOSE_TIME')
#     SWAPS = Col('SWAPS')
#     PROFIT = Col('PROFIT')
#     COMMENT = Col('COMMENT')
#     GROUP = Col('GROUP')
