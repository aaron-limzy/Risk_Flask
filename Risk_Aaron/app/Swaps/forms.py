

from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField, HiddenField, FloatField, FormField, IntegerField, DecimalField, RadioField
from wtforms.validators import DataRequired, ValidationError, Email, EqualTo, Length, NumberRange, InputRequired, AnyOf
from flask_wtf.file import FileField, FileAllowed, FileRequired
from flask_uploads import UploadSet, IMAGES, configure_uploads
from app import excel_format


class UploadForm(FlaskForm):
    #recipe_title = StringField('Recipe Title', validators=[DataRequired()])
    #recipe_description = StringField('Recipe Description', validators=[DataRequired()])
    #recipe_image = FileField('Recipe Image', validators=[FileRequired(), FileAllowed(images, 'Excel Files only!')])
    upload = FileField('Swap Excel File: ', validators=[FileRequired(), FileAllowed(excel_format,  'CSV Files only!')])
    submit = SubmitField('Upload')