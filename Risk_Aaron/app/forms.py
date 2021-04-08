from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField, HiddenField, FloatField, FormField, IntegerField, DecimalField, RadioField, SelectField, SelectMultipleField
from wtforms.validators import DataRequired, ValidationError, Email, EqualTo, Length, NumberRange, InputRequired, AnyOf
from flask_wtf.file import FileField, FileAllowed, FileRequired
from flask_uploads import UploadSet, IMAGES, configure_uploads
from app.extensions import excel_format



import json
from app.models import *

# Configure the image uploading via Flask-Uploads
#images = UploadSet('files',  extensions=('xls', 'xlsx', 'csv'))


class MT5_Modify_Trades_Form(FlaskForm):
    MT5_Login = IntegerField('User Login', validators=[DataRequired()])
    MT5_Deal_Num = IntegerField('Deal Number', validators=[DataRequired()])
    MT5_Comment = StringField('Comment')
    MT5_Action = RadioField("Action", choices=[("delete",'Delete'),("change comment",'Change Comment')], validators=[DataRequired()])
    #upload = FileField('Excels', validators=[FileAllowed(excel_format,  'CSV Files only!')])
    submit = SubmitField('Upload')

class File_Form(FlaskForm):
    upload = FileField('Excels', validators=[FileAllowed(excel_format,  'CSV Files only!')])



class SymbolSwap(FlaskForm):
    Symbol = StringField('Symbol')
    Short = FloatField('Short', validators=[DataRequired()])
    Long = FloatField('Long', validators=[DataRequired()])


class SymbolTotal(FlaskForm):
    Symbol_individual = FormField(SymbolSwap)


class SymbolTotalTest(FlaskForm):
    submit = SubmitField('Upload')

    @classmethod
    def append_field(cls, name, field):
        setattr(cls, name, field)
        return cls


class AddOffSet(FlaskForm):
    Symbol = StringField('Symbol', validators=[DataRequired()])
    Offset = DecimalField('Offset BGI Lots', places=2,rounding=None,  validators=[InputRequired()], description = "Opposite direction from LP.")
    Ticket = IntegerField("BGI Ticket #", validators=[DataRequired()],description = "Ticket number of MT4. (Required)")
    LP =  StringField('LP (Trade to which LP)', validators=[DataRequired()],description = "LP which trade was placed on to.")
    Comment = StringField('Comment (If any)',description = "Any other information.")
    submit = SubmitField('Submit')




# Want to create a SQL insert, for any tables that has Live, Login, Equity_limit
class equity_Protect_Cut(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")], description = "1,2,3 or 5.")
    Login = IntegerField('Login', validators=[DataRequired(message="Only numbers are allowed")], description = "Client Login.")
    Equity_Limit = IntegerField('Equity Limit', validators=[InputRequired(message="Only numbers are allowed")], description = "Equity to cut position, if client equity falls below. Set to 0 for Equity < Credit")
    submit = SubmitField('Submit')


# Want to create a SQL insert, for any tables that has Live, Group
class Live_Group(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")], description = "1,2,3 or 5.")
    Client_Group = StringField('Client Group', validators=[DataRequired(message="Group name Required")], description = "Client Group to include in Risk Auto Cut.")
    submit = SubmitField('Submit')

class Monitor_Account_Trade(FlaskForm):
    Live = IntegerField('Live', validators=[DataRequired(), AnyOf(values=[1,2,3,5], message="Only Live 1,2,3 and 5")], description = "1,2,3 or 5.")
    Account = IntegerField('Account', validators=[DataRequired(message="Only numbers are allowed")], description = "Client Login.")
    Telegram_User  = SelectField("Telegram User", validate_choice=False)
    Email_Risk = BooleanField('Email Risk', description = "If an email needs to be sent.", default=False)
    submit = SubmitField('Submit')

class Monitor_Account_Remove(FlaskForm):
    Monitor_Account  = SelectMultipleField("Monitor Accounts")
    submit = SubmitField('Submit')


