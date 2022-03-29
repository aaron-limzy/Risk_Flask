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


def positive_only(form, field):
    if field.data <= 0 :
        raise ValidationError('Field must be Positive')


# Form to be output into a table
class symbol_form(FlaskForm):

    Symbol = HiddenField("Symbol",render_kw={'readonly': True})
    Spread_Dollar = FloatField('Spread_Dollar', [InputRequired(), positive_only])

     # See if we can do a custom validator for the fields.
    def validate_Spread_Dollar(form, field):
        # print("{} Validating Spread Dollar. field.Spread_Dollar : {}".format(form.Symbol.data, (field.data)))
        # print("digits: {}".format(float(form.digits.data)))
        # print("Minimum: {}".format( 20 * 10 ** (-1 * float(form.digits.data))))

        if float(form.digits.data) != 0:
            min_spread = 20 * 10 ** (-1 * float(form.digits.data))
            max_spread = 500 * 10 ** (-1 * float(form.digits.data))
        else:
            min_spread = 2
            max_spread = 15

        if float(field.data) <= min_spread:
            raise ValidationError("{} 點差太小 , 無法上傳 (最低限度:{})".format(form.Symbol.data, min_spread))

        if float(field.data) >= max_spread:
            raise ValidationError("{} 點差太大 , 無法上傳 (最高限度:{})".format(form.Symbol.data, max_spread))



    Spread_Points = HiddenField('Spread_Points',render_kw={'readonly': True})

    # To keep track if the data has been changed.
    # Spread_Dollar_Hidden = HiddenField('Spread_Dollar_Hidden', render_kw={'readonly': True})
    # Spread_Points_Hidden = HiddenField('Spread_Points_Hidden', render_kw={'readonly': True})
    digits = HiddenField('Digits')

    # Need to have a delicated counter for the sequence
    # To be added for HTML id number.
    counter = HiddenField('counter')





class All_Symbol_Spread_HK_Form(FlaskForm):
    #title = StringField('title')
    core_symbols = FieldList(FormField(symbol_form))

