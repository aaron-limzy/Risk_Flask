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