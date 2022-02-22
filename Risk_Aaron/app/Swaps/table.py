from flask_table import Table, Col, LinkCol, ButtonCol



# Variable name has to be the Dict name.
# Has to provide a list of Dicts.
class Symbol_Swap_Profile_Table(Table):

    Core_Symbol = Col('Core_Symbol')
    Contract_Size = Col('Contract_Size')
    Digits = Col('Digits')
    #Type = Col('Type')
    Currency = Col('Currency')
    Swap_Markup_Profile = Col('Swap_Markup_Profile')
    #
    # #Delete = LinkCol('Remove', endpoint="main_app.edit", url_kwargs=dict(Live='Live', Account='Account'))
    # Delete_Button = ButtonCol('Delete User', endpoint="main_app.Delete_Monitor_Account_Button_Endpoint",
    #                           url_kwargs=dict(Live='Live', Account='Account', Tele_name='Tele_name'),
    #                           button_attrs={"Class" : "btn btn-secondary"})
