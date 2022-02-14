

from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField, HiddenField, FloatField, FormField, IntegerField, DecimalField, RadioField, FieldList
from wtforms.validators import DataRequired, ValidationError, Email, EqualTo, Length, NumberRange, InputRequired, AnyOf
from flask_wtf.file import FileField, FileAllowed, FileRequired
from flask_uploads import UploadSet, IMAGES, configure_uploads
from app.extensions import excel_format

from flask_table import Table, Col, ButtonCol



class UploadForm(FlaskForm):
    #recipe_title = StringField('Recipe Title', validators=[DataRequired()])
    #recipe_description = StringField('Recipe Description', validators=[DataRequired()])
    #recipe_image = FileField('Recipe Image', validators=[FileRequired(), FileAllowed(images, 'Excel Files only!')])
    upload = FileField('Swap Excel File: ', validators=[FileRequired(), FileAllowed(excel_format,  'CSV Files only!')])
    submit = SubmitField('Calculate')

# Form to be output into a table
class Individual_symbol_Form(FlaskForm):
    symbol = HiddenField('Symbol', render_kw={'readonly': True})

    avg_long = HiddenField('avg_long', render_kw={'readonly': True})
    long_style = HiddenField('bg-danger', render_kw={'readonly': True})

    broker_1_long = HiddenField('broker_1_long', render_kw={'readonly': True})
    broker_2_long = HiddenField('broker_2_long', render_kw={'readonly': True})
    broker_1_long_style = HiddenField('', render_kw={'readonly': True})
    broker_2_long_style = HiddenField('', render_kw={'readonly': True})




    short_style = HiddenField('bg-secondary', render_kw={'readonly': True})
    avg_short = HiddenField('avg_short', render_kw={'readonly': True})

    broker_1_short = HiddenField('broker_1_short', render_kw={'readonly': True})
    broker_2_short = HiddenField('broker_2_short', render_kw={'readonly': True})
    broker_1_short_style = HiddenField('', render_kw={'readonly': True})
    broker_2_short_style = HiddenField('', render_kw={'readonly': True})

    bloomberg_dividend = HiddenField('bloomberg_dividend', render_kw={'readonly': True})
    symbol_markup_type = HiddenField('symbol_markup_type', render_kw={'readonly': True})
    symbol_markup_style = HiddenField('symbol_markup_style', render_kw={'readonly': True})


    long = FloatField('Long')
    short = FloatField('Short')

    # To keep track if the Long and Short data has been changed.
    Long_Hidden = HiddenField('Long_Hidden', render_kw={'readonly': True})
    Short_Hidden =  HiddenField('Short_Hidden', render_kw={'readonly': True})

    insti_long = HiddenField('insti_long', render_kw={'readonly': True})
    insti_short = HiddenField('insti_short', render_kw={'readonly': True})

    # etc.


class All_Swap_Form(FlaskForm):
    #title = StringField('title')
    core_symbols = FieldList(FormField(Individual_symbol_Form))


class AddMarkupProfile(FlaskForm):
    #title = StringField('title')
    # core_symbols = FieldList(FormField(Individual_symbol_Form))

    Markup_Profile = StringField('Markup Profile Name', validators=[DataRequired(message="Markup Profile Name Required")], description = "Name for the Markup Profile.")
    Long_Markup = FloatField('Long Markup Percentage', description = "Example: <b>10</b> if a 10% markup is required. (110% of swap uploaded)")
    Short_Markup = FloatField('Short Markup Percentage', description = "Example: <b>10</b> if a 10% markup is required. (110% of swap uploaded)")
    submit = SubmitField('Submit', render_kw={"Class": "btn btn-primary btn-lg btn-block"})


# Variable name has to be the Dict name.
# Has to provide a list of Dicts.
class Delete_Swap_Profile_Table(Table):
    Swap_markup_profile = Col('Swap Markup Profile')
    Long_Markup = Col('Long Markup')
    Short_Markup = Col('Short Markup')
    Delete_Button = ButtonCol('Delete Swap Profile', endpoint="swaps.Remove_Swap_Markup_Profile_Endpoint",
                              url_kwargs=dict(Swap_Profile='Swap_markup_profile'),
                              button_attrs={"Class": "btn btn-secondary"})



# class Upload_File_Form(FlaskForm):
#     file = FileField()

# class Individual_symbol_Form_2(FlaskForm):
#     symbol = StringField('Symbol')
#     #avg_long = FloatField('avg_long')
#     long = FloatField('Long')
#     #long_style = StringField('bg-danger')
#     short = FloatField('Short')
#     #short_style = StringField('bg-secondary')
#     #avg_short = FloatField('avg_short')
#     #bloomberg_dividend = StringField('bloomberg_dividend')
#
#     #broker_long = FloatField('broker_long')
#
#     #broker_short = FloatField('broker_short')
#
# class All_Swap_Form_2(FlaskForm):
#     # title = StringField('title')
#     core_symbols = FieldList(FormField(Individual_symbol_Form_2))
#
#
