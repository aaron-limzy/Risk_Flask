from flask_table import Table, Col, LinkCol, ButtonCol



# Variable name has to be the Dict name.
# Has to provide a list of Dicts.
class Delete_Risk_Autocut_Include_Table(Table):
    Live = Col('LIVE')
    Login = Col('LOGIN')
    Equity_limit=Col('EQUITY LIMIT')
    Group = Col('GROUP')
    Enable = Col('ENABLE')
    Enable_readonly = Col('READ ONLY')
    Balance = Col('BALANCE')
    Credit = Col('CREDIT')
    Equity = Col('EQUITY')
    Delete_Button = ButtonCol('Delete', endpoint="Risk_Client_Tools_bp.Delete_Risk_Autocut_Include_Button_Endpoint",
                              url_kwargs=dict(Live='Live', Login='Login'),
                              button_attrs={"Class" : "btn btn-secondary"})

# Variable name has to be the Dict name.
# Has to provide a list of Dicts.
class Delete_Risk_Autocut_Group_Table(Table):
    Live = Col('LIVE')
    Group = Col('GROUP')
    Num_users = Col('USER COUNT')
    Sum_balance = Col('SUM OF BALANCE')
    Sum_credit = Col('SUM OF CREDIT')
    Delete_Button = ButtonCol('Delete', endpoint="Risk_Client_Tools_bp.Delete_Risk_Autocut_Group_Button_Endpoint",
                              url_kwargs=dict(Live='Live', Group='Group'),
                              button_attrs={"Class": "btn btn-secondary"})


# Variable name has to be the Dict name.
# Has to provide a list of Dicts.
class Delete_Risk_Autocut_Exclude_Table(Table):
    Live = Col('LIVE')
    Login = Col('LOGIN')
    Group = Col('GROUP')
    Balance = Col('BALANCE')
    Credit = Col('CREDIT')
    Equity = Col('EQUITY')
    Enable = Col('ENABLE')
    Enable_readonly = Col('READ ONLY')

    Delete_Button = ButtonCol('Delete', endpoint="Risk_Client_Tools_bp.Delete_Risk_Autocut_Exclude_Button_Endpoint",
                              url_kwargs=dict(Live='Live', Login='Login'),
                              button_attrs={"Class": "btn btn-secondary"})

