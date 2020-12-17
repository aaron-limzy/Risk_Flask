from flask_table import Table, Col, LinkCol, ButtonCol



# Table to show what's on SQL so that we can delete it.
# Shows the clients and symbols that we are currently tracking.
class Delete_Client_No_Trades_Table(Table):
    Live = Col('LIVE')
    Login = Col('LOGIN')
    Symbol=Col('Symbol')
    Delete_Button = ButtonCol('Delete', endpoint="notifications_bp.Delete_Risk_Autocut_Include_Button_Endpoint",
                              url_kwargs=dict(Live='Live', Login='Login', Symbol='Symbol'),
                              button_attrs={"Class" : "btn btn-secondary"})