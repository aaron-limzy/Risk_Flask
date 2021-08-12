from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField, HiddenField, FloatField, FormField, IntegerField, \
    FieldList, DecimalField, RadioField, SelectField, SelectMultipleField
from wtforms.validators import DataRequired, ValidationError, Email, EqualTo, Length, NumberRange, InputRequired, AnyOf
from flask_wtf.file import FileField, FileAllowed, FileRequired
from flask_uploads import UploadSet, IMAGES, configure_uploads
from app.extensions import excel_format



import json
from app.models import *

# Configure the image uploading via Flask-Uploads
#images = UploadSet('files',  extensions=('xls', 'xlsx', 'csv'))



# Want to create a SQL insert, for any tables that has Live, Login, Equity_limit
class equity_Protect_Cut(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")], description = "1,2,3 or 5.")
    Login = IntegerField('Login', validators=[DataRequired(message="Only numbers are allowed")], description = "Client Login.")
    Equity_Limit = IntegerField('Equity Limit', validators=[InputRequired(message="Only numbers are allowed")], description = "Equity to cut position, if client equity falls below. Set to 0 for Equity < Credit")
    submit = SubmitField('Submit')

# Want to create a SQL insert, for any tables that has Live, Login, Equity_limit
class Live_Client_Submit(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")], description = "1,2,3 or 5.")
    Login = IntegerField('Login', validators=[DataRequired(message="Only numbers are allowed")], description = "Client Login.")
    submit = SubmitField('Submit')


# Want to create a SQL insert, for any tables that has Live, Group
class Live_Group(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")], description = "1,2,3 or 5.")
    Client_Group = StringField('Client Group', validators=[DataRequired(message="Group name Required")], description = "Client Group to include in Risk Auto Cut.")
    submit = SubmitField('Submit')


# Want to add into SQL for change group, when there are no open trades.
class noTrade_ChangeGroup_Form(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")])
    Login = IntegerField('Login', validators=[DataRequired()])
    Current_Group = StringField('Current Group', validators=[DataRequired()])
    New_Group = StringField('New Group', validators=[DataRequired()])
    submit = SubmitField('Submit')



# Form to be output into a table
class symbol_form(FlaskForm):

    Symbol = StringField("Symbol")
    Spread_Dollar = FloatField('Spread_Dollar')
    Spread_Points = FloatField('Spread_Points')

    # To keep track if the data has been changed.
    Spread_Dollar_Hidden = HiddenField('Spread_Dollar_Hidden', render_kw={'readonly': True})
    Spread_Points_Hidden = HiddenField('Spread_Points_Hidden', render_kw={'readonly': True})



class All_Symbol_Spread_HK_Form(FlaskForm):
    #title = StringField('title')
    core_symbols = FieldList(FormField(symbol_form))

