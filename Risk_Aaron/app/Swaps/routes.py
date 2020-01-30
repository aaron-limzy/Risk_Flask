from flask import Blueprint, render_template, Markup, url_for, request
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from app.Swaps.A_Utils import *
from sqlalchemy import text
from app import app, db, excel

from app.Swaps.forms import UploadForm

from flask_table import create_table, Col

import requests

import pyexcel

from werkzeug.utils import secure_filename

swaps = Blueprint('swaps', __name__)

@swaps.route('/Swaps/BGI_Swaps')
@login_required
def BGI_Swaps():
    description = Markup("Swap values uploaded onto MT4/MT5. <br>\
   Swaps would be charged on the roll over to the next day.<br> \
    Three day swaps would be charged for FX on weds and CFDs on fri. ")

    return render_template("Standard_Single_Table.html", backgroud_Filename='css/Faded_car.jpg', Table_name="BGI_Swaps", \
                           title="BGISwaps", ajax_url=url_for("swaps.BGI_Swaps_ajax"),
                           description=description, replace_words=Markup(["Today"]))



@swaps.route('/Swaps/BGI_Swaps_ajax', methods=['GET', 'POST'])
@login_required
def BGI_Swaps_ajax():     # Return the Bloomberg dividend table in Json.

    start_date = get_working_day_date(datetime.date.today(), -1, 5)
    sql_query = text("SELECT * FROM test.`bgi_swaps` where date >= '{}' ORDER BY Core_Symbol, Date".format(start_date.strftime("%Y-%m-%d")))
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()
    result_col = raw_result.keys()
    return_result = [dict(zip(result_col, d)) for d in result_data]



    # Want to get unuqie dates, and sort them.
    unique_date = list(set([d['Date'] for d in return_result if 'Date' in d]))
    unique_date.sort()

    # Want to get unuqie Symbol, and sort them.
    unique_symbol = list(set([d['Core_Symbol'] for d in return_result if 'Core_Symbol' in d]))
    unique_symbol.sort()


    swap_total = [] # To store all the symbol
    for s in unique_symbol:
        swap_long_buffer = dict()
        swap_short_buffer = dict()
        swap_long_buffer['Symbol'] = s  # Save symbol name
        swap_long_buffer['Direction'] = "Long"
        swap_short_buffer['Symbol'] = s
        swap_short_buffer['Direction'] = "Short"
        for d in unique_date:       # Want to get Values of short and long.
            swap_long_buffer[d.strftime("%Y-%m-%d (%a)")] = find_swaps(return_result, s, d, "bgi_long")
            swap_short_buffer[d.strftime("%Y-%m-%d (%a)")] = find_swaps(return_result, s, d, "bgi_short")
        swap_total.append(swap_long_buffer)
        swap_total.append(swap_short_buffer)

    return json.dumps(swap_total)


@swaps.route('/Swaps/upload_LP_Swaps', methods=['GET', 'POST'])
def upload_excel():

    title = "Swaps Upload"
    header = "Swaps Upload"
    description = Markup("Swaps<br>Upload")
    form = UploadForm()
    if request.method == 'POST' and form.validate_on_submit():

        record_dict = request.get_records(field_name='upload', name_columns_by_row=0)

        month_year = datetime.datetime.now().strftime('%b-%Y')
        month_year_folder = app.config["VANTAGE_UPLOAD_FOLDER"] + "/" + month_year

        filename = secure_filename(request.files['upload'].filename)

        filename_postfix_xlsx = Check_File_Exist(month_year_folder, ".".join(
            filename.split(".")[:-1]) + ".xlsx")  # Checks, Creates folders and return AVAILABLE filename

        # Want to Let the users download the File..
        # return excel.make_response_from_records(record_dict, "xls", status=200, file_name=filename_without_postfix)

        pyexcel.save_as(records=record_dict, dest_file_name=filename_postfix_xlsx)


        column_name = []
        file_data = []
        for cc, record in enumerate(record_dict):
            if cc == 0:
                column_name = list(record_dict[cc].keys())
            buffer = dict()
            #print(record)
            for i, j in record.items():
                if i == "":
                    i = "Empty"
                buffer[i] = j
                #print(i, j)
            file_data.append(buffer)
        print(file_data)
        T = create_table()
        table = T(file_data, classes=["table", "table-striped", "table-bordered", "table-hover"])
        if (len(file_data) > 0) and isinstance(file_data[0], dict):
            for c in file_data[0]:
                if c != "\n":
                    table.add_column(c, Col(c, th_html_attrs={"style": "background-color:# afcdff"}))
        #
        #return render_template("upload_form.html", form=form, table=table)
        return render_template("Webworker_Single_Table.html", backgroud_Filename='css/Charts.jpg',
                               form=form, Table_name="Swaps", header=header, description=description, title=title,table=table)

    return render_template("Webworker_Single_Table.html",  backgroud_Filename='css/Charts.jpg',
                           form=form, Table_name="Swaps", header=header, description=description, title=title)
