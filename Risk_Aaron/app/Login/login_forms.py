
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, SelectField, BooleanField
from wtforms.validators import DataRequired, Email, EqualTo, InputRequired


# For Creating User Form. Need them to submit password twice.
class CreateUserForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    email =  StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('New Password', [InputRequired(), EqualTo('confirm', message='Passwords must match')])
    confirm  = PasswordField('Repeat Password')
    #role = SelectField("Role", choices=[("Risk", "Risk"), ("Risk_TW", "Risk_TW"), ("Finance", "Finance"), ("Dealing", "Dealing"), ("Others", "Others")])
    role = SelectField("Role", validate_choice=False)
    submit = SubmitField('Submit')


class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign In')


# For Letting user change password
class EditDetailsForm(FlaskForm):
    email =  StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('New Password', [InputRequired(), EqualTo('confirm', message='Passwords must match')])
    confirm  = PasswordField('Repeat Password')
    submit = SubmitField('Submit')

# For Letting user change password
class Admin_EditDetailsForm(FlaskForm):
    username = SelectField("Username", validate_choice=False)
    password = PasswordField('New Password', [InputRequired(), EqualTo('confirm', message='Passwords must match')])
    confirm  = PasswordField('Repeat Password')
    submit = SubmitField('Submit')
