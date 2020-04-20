from flask import Blueprint, render_template, Markup, url_for, request
from flask_login import current_user, login_user, logout_user, login_required

from Aaron_Lib import *
from app.Swaps.A_Utils import *
from sqlalchemy import text
from app.extensions import db, excel

from app.Swaps.forms import UploadForm

from app.Swaps.get_swaps_all import *

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
    Three day swaps would be charged for FX on weds and CFDs on fri.<br>" +
                         "Swaps are saved on 64.73 test.bgi_swaps table")

    return render_template("Standard_Single_Table.html", backgroud_Filename='css/Faded_car.jpg', Table_name="BGI_Swaps", \
                           title="BGISwaps", ajax_url=url_for("swaps.BGI_Swaps_ajax"),
                           description=description, replace_words=Markup(["Today"]))



@swaps.route('/Swaps/BGI_Swaps_ajax', methods=['GET', 'POST'])
@login_required
def BGI_Swaps_ajax():     # Return the Bloomberg dividend table in Json.

    start_date = get_working_day_date(datetime.date.today(), -5)
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


@swaps.route('/Bloomberg_Dividend')
@login_required
def Bloomberg_Dividend():
    description = Markup("Dividend Values in the table above are 1-day early, when the values are uploaded as swaps onto MT4. <br>\
    Dividend would be given out/charged the next working day.")
    return render_template("Standard_Single_Table.html", backgroud_Filename='css/Charts.jpg', Table_name="CFD Dividend", \
                           title="CFD Dividend", ajax_url=url_for("swaps.Bloomberg_Dividend_ajax"),
                           description=description, replace_words=Markup(["Today"]))



@swaps.route('/Bloomberg_Dividend_ajax', methods=['GET', 'POST'])
@login_required
def Bloomberg_Dividend_ajax():     # Return the Bloomberg dividend table in Json.

    start_date = get_working_day_date(datetime.date.today(), -3)
    end_date = get_working_day_date(datetime.date.today(), 5)

    sql_query = text("Select * from aaron.bloomberg_dividend WHERE `date` >= '{}' and `date` <= '{}' AND WEEKDAY(`date`) BETWEEN 0 AND 4 ORDER BY date".format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))

    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return [('A50', 'XIN9I Index', 0.0, datetime.date(2019, 8, 14), datetime.datetime(2019, 8, 14, 8, 0, 14)), ....]

    # Want to get all the weekdays in the middle of start_date and end_date.
    all_dates = [start_date + datetime.timedelta(days=d) for d in range(abs((end_date - start_date).days)) \
                 if (start_date + datetime.timedelta(days=d)).weekday() in range(5)]

    # dict of the results
    result_col = raw_result.keys()
    result_dict = [dict(zip(result_col,d)) for d in result_data]

    # Get unique symbols using set, then sort them.
    symbols = list(set([rd[0] for rd in result_data]))
    symbols.sort()
    return_data = []   # The data to be returned.

    for s in symbols:
        symbol_dividend_date = dict()
        symbol_dividend_date["Symbol"] = s
        for d in all_dates:
            symbol_dividend_date[get_working_day_date(start_date=d, weekdays_count=-1).strftime( \
                "%Y-%m-%d")] =  find_dividend(result_dict, s, d)
        return_data.append(symbol_dividend_date)
    return json.dumps(return_data)



@swaps.route('/Swaps/upload_LP_Swaps', methods=['GET', 'POST'])
def upload_excel():

    title = "Swaps Upload"
    header = "Swaps Upload"
    description = Markup("Swaps<br>Upload")
    form = UploadForm()
    if request.method == 'POST' and form.validate_on_submit():

        record_dict = request.get_records(field_name='upload', name_columns_by_row=0)

        month_year = datetime.datetime.now().strftime('%b-%Y')
        month_year_folder = swaps.config["VANTAGE_UPLOAD_FOLDER"] + "/" + month_year

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
                #print(i, j)
            file_data.append(buffer)
        print(file_data)
        T = create_table()
        # table = T(file_data, classes=["table", "table-striped", "table-bordered", "table-hover"])
        # if (len(file_data) > 0) and isinstance(file_data[0], dict):
        #     for c in file_data[0]:
        #         if c != "\n":
        #             table.add_column(c, Col(c, th_html_attrs={"style": "background-color:# afcdff"}))
        #

        table_col = list(file_data[0].keys())
        table_values = [list(d.values()) for d in file_data]

        #return render_template("upload_form.html", form=form, table=table)
        return render_template("Single_Table_FixedBG.html", backgroud_Filename='css/Charts.jpg',
                               form=form, Table_name="Swaps", header=header, description=description, title=title, html=Markup(Array_To_HTML_Table(table_col, table_values,
                                            Table_Class=['table', 'table-striped', 'table-hover', 'table-bordered', 'table-light', 'table-sm'])))

    return render_template("Single_Table_FixedBG.html",  backgroud_Filename='css/Charts.jpg',
                           form=form, Table_name="Swaps", header=header, description=description, title=title,
                           html=Markup(Array_To_HTML_Table(Table_Header=[str(i) for i in range(20)], Table_Data=[["{:.4f}".format(random.random()) for i in range(20)] for j in range(100)],
                                                           Table_Class=['table', 'table-striped', 'table-hover', 'table-bordered', 'table-light', 'table-sm'])))


@swaps.route('/Swaps/Other_Brokers', methods=['GET', 'POST'])
def Other_Brokers():

    title = "Swap Compare"
    header = "Swap Compare"



    description = Markup("Swaps from other brokers.<br>" +
                         "fxdd: fxdd (https://www.fxdd.com/mt/en/trading/offering)<br>" +
                         "fdc: Forex.com (https://www.forex.com/en/trading/pricing-fees/rollover-rates/)<br>" +
                         "tv: Trade-View(https://www.tradeviewforex.com/room/forex-resources/rollover-rates)<br>" +
                         "gp: Global Prime (https://www.globalprime.com/trading-conditions/swaps-financing/)<br>" +
                         "saxo: saxo (https://www.home.saxo/en-sg/rates-and-conditions/forex/trading-conditions#historic-swap-points)<br>" +
                         "ebh: European Brokerage House (https://ebhforex.com/faq/rollover-policy/)<br>" +
                         "fpm: fpmarkets (https://www.fpmarkets.com/swap-point)<br>")

        # TODO: Add Form to add login/Live/limit into the exclude table.
    return render_template("Webworker_Single_Table_No_Border.html", backgroud_Filename='css/Person_Mac.jpg', icon="",
                           Table_name="Swap Compare ", title=title,
                            ajax_url=url_for('swaps.Other_Brokers_Ajax', _external=True), header=header,
                           description=description, replace_words=Markup(["Today"]))





@swaps.route('/Swaps/Other_Brokers_Ajax', methods=['GET', 'POST'])
def Other_Brokers_Ajax():
    df_other_broker_swaps = get_broker_swaps()


    sql_query_line = """select Core_Symbol as Symbol
    FROM aaron.swap_bgicoresymbol
    ORDER BY Symbol"""

    df_bgi_core_symbol = get_from_sql_or_file(sql_query_line, "app\\Swaps\\Upload_Swaps\\BGI_Core_Symbol_Only.xls", db)
    df_other_broker_swaps = df_bgi_core_symbol.merge(df_other_broker_swaps, on="Symbol", how="left")
    #print(df_bgi_core_symbol)
    #print("Other Brokers")
    #print(df_other_broker_swaps)
    df_other_broker_swaps.fillna("-", inplace=True)

    #return json.dumps(pd_dataframe_to_dict(df_fdc))

    return json.dumps(pd_dataframe_to_dict(df_other_broker_swaps))
    #return json.dumps([{"Testing": "12345"}])









# @app.route('/upload')
# def upload_file2():
#  return '''
# <html>
# <body>
#    <form action = "http://localhost:5000/uploader" method = "POST"
#       enctype = "multipart/form-data">
#       <input type = "file" name = "file" />
#       <input type = "submit"/>
#    </form>
# </body>
# </html>
#  '''

#
# @app.route('/uploader', methods=['GET', 'POST'])
# def upload_file():
#  if request.method == 'POST':
#      f = request.files['file']
#      f.save(secure_filename(f.filename))
#      return 'file uploaded successfully'
#
#
# @app.route('/uploads/<filename>')
# def uploaded_file(filename):
#  return send_from_directory(app.config['UPLOAD_FOLDER'],
#                             filename)



#
# @app.route('/test1', methods=['GET', 'POST'])
# def retrieve_db_swaps():
#  raw_result = db.engine.execute("select * from aaron.swap_bgi_vantage_coresymbol")
#  result_data = raw_result.fetchall()
#  result_col = raw_result.keys()
#  result_colate = [dict(zip(result_col, a)) for a in result_data]
#
#  T = create_table()
#  table = T(result_colate, classes=["table", "table-striped", "table-bordered", "table-hover"])
#  if (len(result_colate) > 0) and isinstance(result_colate[0], dict):
#      for c in result_colate[0]:
#          if c != "\n":
#              table.add_column(c, Col(c, th_html_attrs={"style": "background-color:# afcdff"}))
#  return render_template("Swap_Sql.html", table=table)

#
# @app.route('/upload_swap', methods=['GET', 'POST'])
# def retrieve_db_swaps2():
#  form = UploadForm()
#
#  if request.method == 'POST' and form.validate_on_submit():
#
#      record_dict = request.get_records(field_name='upload', name_columns_by_row=0)
#      # record_dict = request.get_records(field_name='upload')
#      month_year = datetime.now().strftime('%b-%Y')
#      month_year_folder = app.config["VANTAGE_UPLOAD_FOLDER"] + "/" + month_year
#
#      filename = secure_filename(request.files['upload'].filename)
#
#      filename_postfix_xlsx = Check_File_Exist(month_year_folder, ".".join(
#          filename.split(".")[:-1]) + ".xlsx")  # Checks, Creates folders and return AVAILABLE filename
#
#      # Want to Let the users download the File..
#      # return excel.make_response_from_records(record_dict, "xls", status=200, file_name=filename_without_postfix)
#
#      # pyexcel.save_as(records=record_dict, dest_file_name=filename_postfix_xlsx)
#
#      column_name = []
#      file_data = []
#      for cc, record in enumerate(record_dict):
#          if cc == 0:
#              column_name = list(record_dict[cc].keys())
#          buffer = dict()
#          # print(record)
#          for i, j in record.items():
#              if i == "":
#                  i = "Empty"
#              buffer[str(i).strip()] = str(j).strip()
#              print(i, j)
#          file_data.append(buffer)
#
#      raw_result = db.engine.execute("\
#          select `BGI_CORE`.`BGI_CORE_SYMBOL`,`BGI_VANTAGE`.`VANTAGE_CORE_SYMBOL`,`BGI_CORE`.`BGI_TYPE` , \
#          `BGI_CORE`.`BGI_CONTRACT_SIZE` , `BGI_CORE`.`BGI_DIGITS` , `VANTAGE_CORE`.`VANTAGE_DIGITS`,`VANTAGE_CORE`.`VANTAGE_CONTRACT_SIZE`, \
#          `BGI_CORE`.`BGI_POSITIVE_MARKUP`, `BGI_CORE`.`BGI_NEGATIVE_MARKUP`, `BGI_CORE`.currency, \
#          `BGI_FORCED`.`FORCED_BGI_LONG`, `BGI_FORCED`.`FORCED_BGI_SHORT`,  \
#          `BGI_FORCED_INSTI`.`BGI_INSTI_FORCED_LONG`,`BGI_FORCED_INSTI`.`BGI_INSTI_FORCED_SHORT` \
#          from \
#          (Select core_symbol as `BGI_CORE_SYMBOL`, contract_size as `BGI_CONTRACT_SIZE`, digits as `BGI_DIGITS`,  \
#          type as `BGI_TYPE`, positive_markup as `BGI_POSITIVE_MARKUP`, negative_markup as `BGI_NEGATIVE_MARKUP` ,currency \
#          from swap_bgicoresymbol) as `BGI_CORE` \
#          LEFT JOIN \
#          (Select bgi_coresymbol as `BGI_CORESYMBOL`, vantage_coresymbol as `VANTAGE_CORE_SYMBOL`  \
#          from aaron.swap_bgi_vantage_coresymbol) as `BGI_VANTAGE` on BGI_CORE.BGI_CORE_SYMBOL = BGI_VANTAGE.BGI_CORESYMBOL \
#          LEFT JOIN \
#          (Select core_symbol as `VANTAGE_CORESYMBOL`,contract_size as `VANTAGE_CONTRACT_SIZE`, digits as `VANTAGE_DIGITS`  \
#          from aaron.swap_vantagecoresymbol) as `VANTAGE_CORE` on `VANTAGE_CORE`.`VANTAGE_CORESYMBOL` = `BGI_VANTAGE`.`VANTAGE_CORE_SYMBOL` \
#          LEFT JOIN \
#          (Select core_symbol as `FORCED_BGI_CORESYMBOL`,`long` as `FORCED_BGI_LONG`, short as `FORCED_BGI_SHORT`  \
#          from aaron.swap_bgiforcedswap) as `BGI_FORCED` on BGI_CORE.BGI_CORE_SYMBOL = BGI_FORCED.FORCED_BGI_CORESYMBOL \
#          LEFT JOIN \
#          (Select core_symbol as `BGI_INSTI_FORCED_CORESYMBOL`, `long` as `BGI_INSTI_FORCED_LONG`, short as `BGI_INSTI_FORCED_SHORT`  \
#          from aaron.swap_bgiforcedswap_insti) as `BGI_FORCED_INSTI` on `BGI_CORE`.BGI_CORE_SYMBOL = `BGI_FORCED_INSTI`.BGI_INSTI_FORCED_CORESYMBOL \
#          order by FIELD(`BGI_CORE`.BGI_TYPE, 'FX','Exotic Pairs', 'PM', 'CFD'), BGI_CORE.BGI_CORE_SYMBOL \
#      ")
#
#      result_data = raw_result.fetchall()
#      result_col = raw_result.keys()
#      result_col_no_duplicate = []
#      for a in result_col:
#          if not a in result_col_no_duplicate:
#              result_col_no_duplicate.append(a)
#          else:
#              result_col_no_duplicate.append(str(a)+"_1")
#
#      collate = [dict(zip(result_col_no_duplicate, a)) for a in result_data]
#
#      # Calculate the Markup, as well as acount for the difference in Digits.
#      for i, c in enumerate(collate):     # By Per Row
#          collate[i]["SWAP_UPLOAD_LONG"] = ""         # We want to add the following..
#          collate[i]["SWAP_UPLOAD_LONG_MARKUP"] = ""
#          collate[i]["SWAP_UPLOAD_SHORT"] = ""
#          collate[i]["SWAP_UPLOAD_SHORT_MARKUP"] = ""
#
#          bgi_digit_difference = 0        # Sets as 0 for default.
#          if ("BGI_DIGITS"  in collate[i]) and ("VANTAGE_DIGITS" in collate[i]) and (collate[i]["BGI_DIGITS"] != None) and (collate[i]["VANTAGE_DIGITS"] != None):      # Need to calculate the Digit Difference.
#              bgi_digit_difference = int(collate[i]["BGI_DIGITS"]) - int(collate[i]["VANTAGE_DIGITS"])
#
#
#
#          # print(str(collate[i]['VANTAGE_CORE_SYMBOL']))
#
#          # Retail_Sheet.Cells(i, 2).Value = Find_Retail(Core_Symbol_BGI, 2, Cell_Color) * (10 ^ (Symbol_BGI_Digits - Symbol_Vantage_Digits))
#
#
#
#          for j, d in enumerate(file_data):
#              if 'VANTAGE_CORE_SYMBOL' in collate[i] and \
#                      len(list(d.keys())) >= 1 and \
#                      str(collate[i]['VANTAGE_CORE_SYMBOL']).strip() == d[list(d.keys())[0]]:
#
#                  d_key = [str(a).strip() for a in list(d.keys())]        # Get the keys for the Uploaded data.
#
#
#
#                  for ij, coll in enumerate(d_key):
#                      if "buy" in str(coll).lower() or "long" in str(coll).lower():   # Search for buy or long
#                          # Need to check if can be float.
#                          collate_keys = list(collate[i].keys())
#                          if "BGI_POSITIVE_MARKUP" in collate_keys and "BGI_NEGATIVE_MARKUP" in collate_keys and Check_Float(d[coll]):         # If posive, markup with positive markup. if negative, use negative markup.
#                              val = str(round(markup_swaps(float(str(d[coll])), collate[i]["BGI_POSITIVE_MARKUP"], collate[i]["BGI_NEGATIVE_MARKUP"])  * (10 ** bgi_digit_difference),4))  # Does the Digit Conversion here.
#                          else:
#                              val = ""
#                              flash("Unable to calculate markup prices for {}".format(collate[i]["BGI_CORE_SYMBOL"]))     # Put a message out that there is some error.
#
#                          collate[i]["SWAP_UPLOAD_LONG"] = str(d[coll])
#                          collate[i]["SWAP_UPLOAD_LONG_MARKUP"] = val
#
#
#                      if "sell" in str(coll).lower() or "short" in str(coll).lower(): # Search for sell or short in the colum names.
#
#                          # Need to check if can be float.
#                          collate_keys = list(collate[i].keys())
#                          if "BGI_POSITIVE_MARKUP" in collate_keys and "BGI_NEGATIVE_MARKUP" in collate_keys and Check_Float(d[coll]):    # If posive, markup with positive markup. if negative, use negative markup.
#                              val = str( round(markup_swaps(float(str(d[coll])), collate[i]["BGI_POSITIVE_MARKUP"], collate[i]["BGI_NEGATIVE_MARKUP"]) * (10 ** bgi_digit_difference)  ,4) ) # Does the Digit Conversion here.
#                          else:
#                              val = ""
#
#                          collate[i]["SWAP_UPLOAD_SHORT"] = str(d[coll])
#                          collate[i]["SWAP_UPLOAD_SHORT_MARKUP"] = val
#                  # print(d_key[0])
#                  break
#
#
#      # To Create the BGI Swaps.
#      bgi_long_swap_column = ["FORCED_BGI_LONG", "SWAP_UPLOAD_LONG_MARKUP"]       # To get the swap values in that order.
#      bgi_short_swap_column = ["FORCED_BGI_SHORT", "SWAP_UPLOAD_SHORT_MARKUP"]    # If there are forced swaps, get forced first.
#
#      bgi_insti_long_swap_column = ["FORCED_BGI_LONG", "BGI_INSTI_FORCED_LONG", "SWAP_UPLOAD_LONG_MARKUP"]    # If there are forced, if there are any Insti Forced, then the markup values.
#      bgi_insti_short_swap_column = ["FORCED_BGI_SHORT", "BGI_INSTI_FORCED_SHORT", "SWAP_UPLOAD_SHORT_MARKUP"]
#
#
#      bgi_swaps_retail = []
#      bgi_swaps_insti = []
#      for c in collate:
#          buffer_retail = {"SYMBOL": c["BGI_CORE_SYMBOL"], "LONG" : "", "SHORT" : ""}
#          buffer_insti ={"SYMBOL": c["BGI_CORE_SYMBOL"], "LONG" : "", "SHORT" : ""}
#
#          for l in bgi_long_swap_column:      # Going by Precedence.
#              if l in c and c[l] != None:
#                  buffer_retail["LONG"] = c[l]
#                  break
#          for s in bgi_short_swap_column:      # Going by Precedence.
#              if s in c and c[s] != None:
#                  buffer_retail["SHORT"] = c[s]
#                  break
#          for li in bgi_insti_long_swap_column:      # Going by Precedence.
#              if li in c and c[li] != None:
#                  buffer_insti["LONG"] = c[li]
#                  break
#          for si in bgi_insti_short_swap_column:      # Going by Precedence.
#              if si in c and c[si] != None:
#                  buffer_insti["SHORT"] = c[si]
#                  break
#
#          if buffer_retail["LONG"] == "" or buffer_retail["SHORT"] == "" or buffer_insti["LONG"] == "" or buffer_insti["SHORT"] == "" :
#              flash("{} has no Swaps.".format(c["BGI_CORE_SYMBOL"]))  # Flash out if there are no swaps found.
#
#          bgi_swaps_retail.append(buffer_retail)
#          bgi_swaps_insti.append(buffer_insti)
#
#
#      collate_table = [dict(zip([str(a).replace("_", " ") for a in c.keys()], c.values())) \
#                       for c in collate]
#
#
#      table = create_table_fun(collate_table)
#
#      table_bgi_swaps_retail = create_table_fun(bgi_swaps_retail)
#      table_bgi_swaps_insti = create_table_fun(bgi_swaps_insti)
#
#
#
#      return render_template("upload_form.html", table=table, form=form, table_bgi_swaps_retail=table_bgi_swaps_retail, table_bgi_swaps_insti=table_bgi_swaps_insti)
#
#  return render_template("upload_form.html", form=form)


# @app.route('/upload_swap3', methods=['GET', 'POST'])
# def retrieve_db_swaps3():
#  form = UploadForm()
#
#  if request.method == 'POST' and form.validate_on_submit():
#
#      # Get the file details as Records.
#      record_dict = request.get_records(field_name='upload', name_columns_by_row=0)
#
#      # ------------------ Dataframe of the data from CSV File. -----------------
#      df_csv = pd.DataFrame(record_dict)
#
#      uploaded_excel_data = df_csv.fillna("-").rename(columns=dict(zip(df_csv.columns,[a.replace("_","\n") for a in df_csv.columns]))).to_html(classes="table table-striped table-bordered table-hover table-condensed", index=False)
#
#
#      for i in df_csv.columns:  # Want to see which is Long and which is Short.
#          if "core symbol" in i.lower():
#              df_csv.rename(columns={i: "VANTAGE_CORE_SYMBOL"}, inplace=True) # Need to rename "Core symbol" to note that its from vantage.
#          if "long" in i.lower():
#              df_csv.rename(columns={i: "CSV_LONG"}, inplace=True)
#          if "short" in i.lower():
#              df_csv.rename(columns={i: "CSV_SHORT"}, inplace=True)
#
#
#      raw_result = db.engine.execute("\
#          select `BGI_CORE`.`BGI_CORE_SYMBOL`,`BGI_VANTAGE`.`VANTAGE_CORE_SYMBOL`,`BGI_CORE`.`BGI_TYPE` , \
#          `BGI_CORE`.`BGI_CONTRACT_SIZE` , `BGI_CORE`.`BGI_DIGITS` , `VANTAGE_CORE`.`VANTAGE_DIGITS`,`VANTAGE_CORE`.`VANTAGE_CONTRACT_SIZE`, \
#          `BGI_CORE`.`BGI_POSITIVE_MARKUP`, `BGI_CORE`.`BGI_NEGATIVE_MARKUP`, `BGI_CORE`.currency, \
#          `BGI_FORCED`.`FORCED_BGI_LONG`, `BGI_FORCED`.`FORCED_BGI_SHORT`,  \
#          `BGI_FORCED_INSTI`.`BGI_INSTI_FORCED_LONG`,`BGI_FORCED_INSTI`.`BGI_INSTI_FORCED_SHORT` \
#          from \
#          (Select core_symbol as `BGI_CORE_SYMBOL`, contract_size as `BGI_CONTRACT_SIZE`, digits as `BGI_DIGITS`,  \
#          type as `BGI_TYPE`, positive_markup as `BGI_POSITIVE_MARKUP`, negative_markup as `BGI_NEGATIVE_MARKUP` ,currency \
#          from swap_bgicoresymbol) as `BGI_CORE` \
#          LEFT JOIN \
#          (Select bgi_coresymbol as `BGI_CORESYMBOL`, vantage_coresymbol as `VANTAGE_CORE_SYMBOL`  \
#          from aaron.swap_bgi_vantage_coresymbol) as `BGI_VANTAGE` on BGI_CORE.BGI_CORE_SYMBOL = BGI_VANTAGE.BGI_CORESYMBOL \
#          LEFT JOIN \
#          (Select core_symbol as `VANTAGE_CORESYMBOL`,contract_size as `VANTAGE_CONTRACT_SIZE`, digits as `VANTAGE_DIGITS`  \
#          from aaron.swap_vantagecoresymbol) as `VANTAGE_CORE` on `VANTAGE_CORE`.`VANTAGE_CORESYMBOL` = `BGI_VANTAGE`.`VANTAGE_CORE_SYMBOL` \
#          LEFT JOIN \
#          (Select core_symbol as `FORCED_BGI_CORESYMBOL`,`long` as `FORCED_BGI_LONG`, short as `FORCED_BGI_SHORT`  \
#          from aaron.swap_bgiforcedswap) as `BGI_FORCED` on BGI_CORE.BGI_CORE_SYMBOL = BGI_FORCED.FORCED_BGI_CORESYMBOL \
#          LEFT JOIN \
#          (Select core_symbol as `BGI_INSTI_FORCED_CORESYMBOL`, `long` as `BGI_INSTI_FORCED_LONG`, short as `BGI_INSTI_FORCED_SHORT`  \
#          from aaron.swap_bgiforcedswap_insti) as `BGI_FORCED_INSTI` on `BGI_CORE`.BGI_CORE_SYMBOL = `BGI_FORCED_INSTI`.BGI_INSTI_FORCED_CORESYMBOL \
#          order by FIELD(`BGI_CORE`.BGI_TYPE, 'FX','Exotic Pairs', 'PM', 'CFD'), BGI_CORE.BGI_CORE_SYMBOL \
#      ")
#
#      result_data = raw_result.fetchall()
#      result_col = raw_result.keys()
#      result_col_no_duplicate = []
#      for a in result_col:
#          if not a in result_col_no_duplicate:
#              result_col_no_duplicate.append(a)
#          else:
#              result_col_no_duplicate.append(str(a)+"_1")
#
#      # Pandas data frame for the SQL return for the Symbol details.
#      df_sym_details = pd.DataFrame(data=result_data, columns=result_col_no_duplicate)
#      df_sym_details["DIGIT_DIFFERENCE"] = df_sym_details['BGI_DIGITS'] - df_sym_details['VANTAGE_DIGITS']    # Want to calculate the Digit Difference.
#
#
#      # ---------------------------- Time to merge the 2 Data Frames. ---------------------------------------------
#      combine_df = df_sym_details.merge(df_csv, on=["VANTAGE_CORE_SYMBOL"], how="outer")
#      combine_df = combine_df[pd.notnull(combine_df["BGI_CORE_SYMBOL"])]
#
#      # Need to Flip LONG
#      combine_df["CSV_LONG"] = combine_df[ "CSV_LONG"] * -1  # Vantage sending us Positive = Charged. We need to flip  LONG
#      # Need to correct the number of digits.
#      if "CSV_LONG" in combine_df.columns:
#          combine_df["CSV_LONG_CORRECT_DIGITS"] = combine_df["CSV_LONG"] * (10 ** combine_df["DIGIT_DIFFERENCE"])
#      if "CSV_SHORT" in combine_df.columns:
#          combine_df["CSV_SHORT_CORRECT_DIGITS"] = combine_df["CSV_SHORT"] * (10 ** combine_df["DIGIT_DIFFERENCE"])
#
#      # Want to multiply by the correct markup for each. Want to give less, take more.
#      # Long
#      combine_df["CSV_LONG_CORRECT_DIGITS_MARKUP"] = np.where(combine_df["CSV_LONG_CORRECT_DIGITS"] > 0, round(
#          combine_df["CSV_LONG_CORRECT_DIGITS"] * (1 - (combine_df["BGI_POSITIVE_MARKUP"] / 100)), 3), round(
#          combine_df["CSV_LONG_CORRECT_DIGITS"] * (1 + (combine_df["BGI_NEGATIVE_MARKUP"] / 100)), 3))
#      # Short
#      combine_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"] = np.where(combine_df["CSV_SHORT_CORRECT_DIGITS"] > 0, round(
#          combine_df["CSV_SHORT_CORRECT_DIGITS"] * (1 - (combine_df["BGI_POSITIVE_MARKUP"] / 100)), 3), round(
#          combine_df["CSV_SHORT_CORRECT_DIGITS"] * (1 + (combine_df["BGI_NEGATIVE_MARKUP"] / 100)), 3))
#
#      # The Data Frame used for the Building of the CSV File.
#      build_bgi_swap_df = combine_df[
#          ["BGI_CORE_SYMBOL", "VANTAGE_CORE_SYMBOL", "BGI_POSITIVE_MARKUP", "BGI_NEGATIVE_MARKUP", "FORCED_BGI_LONG",
#           "FORCED_BGI_SHORT", "BGI_INSTI_FORCED_LONG", "BGI_INSTI_FORCED_SHORT", "CSV_LONG_CORRECT_DIGITS_MARKUP",
#           "CSV_SHORT_CORRECT_DIGITS_MARKUP", "BGI_TYPE"]]
#
#      # Want to get either the Forced Swaps, if not, Get the "Vantage corrected digit markup" swaps
#      build_bgi_swap_df.loc[:, "LONG"] = np.where(pd.notnull(build_bgi_swap_df["FORCED_BGI_LONG"]),
#                                                  round(build_bgi_swap_df["FORCED_BGI_LONG"], 3), np.where(
#              pd.notnull(build_bgi_swap_df["CSV_LONG_CORRECT_DIGITS_MARKUP"]),
#              round(build_bgi_swap_df["CSV_LONG_CORRECT_DIGITS_MARKUP"], 3),
#              build_bgi_swap_df["CSV_LONG_CORRECT_DIGITS_MARKUP"]))
#      build_bgi_swap_df.loc[:, "SHORT"] = np.where(pd.notnull(build_bgi_swap_df["FORCED_BGI_SHORT"]),
#                                                   round(build_bgi_swap_df["FORCED_BGI_SHORT"], 3), np.where(
#              pd.notnull(build_bgi_swap_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"]),
#              round(build_bgi_swap_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"], 3),
#              build_bgi_swap_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"]))
#
#      # For Insti, we want to check BGI_FORCED first, if null, check BGI_INSTI_FORCED. If not, use vantage digit change markup swap.
#      build_bgi_swap_df.loc[:, "INSTI_LONG"] = np.where(pd.notnull(build_bgi_swap_df["FORCED_BGI_LONG"]), round(build_bgi_swap_df["FORCED_BGI_LONG"], 3), \
#                                                        np.where( pd.notnull(build_bgi_swap_df["BGI_INSTI_FORCED_LONG"]), round(build_bgi_swap_df["BGI_INSTI_FORCED_LONG"], 3), \
#                                                            np.where(pd.notnull( build_bgi_swap_df["CSV_LONG_CORRECT_DIGITS_MARKUP"]), round(build_bgi_swap_df[ "CSV_LONG_CORRECT_DIGITS_MARKUP"], 3), \
#                                                                     build_bgi_swap_df["CSV_LONG_CORRECT_DIGITS_MARKUP"])))
#
#
#      build_bgi_swap_df.loc[:, "INSTI_SHORT"] = np.where(pd.notnull(build_bgi_swap_df["FORCED_BGI_SHORT"]), round(build_bgi_swap_df["FORCED_BGI_SHORT"], 3), \
#                                                         np.where( pd.notnull(build_bgi_swap_df["BGI_INSTI_FORCED_SHORT"]), round(build_bgi_swap_df["BGI_INSTI_FORCED_SHORT"], 3), \
#                                                             np.where(pd.notnull(build_bgi_swap_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"]), round(build_bgi_swap_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"], 3), \
#                                                                      build_bgi_swap_df["CSV_SHORT_CORRECT_DIGITS_MARKUP"])))
#
#      # Want to find out which of the symbols still have NULL.
#
#      Swap_Error = build_bgi_swap_df[pd.isnull(build_bgi_swap_df["LONG"]) | pd.isnull(build_bgi_swap_df["SHORT"])]
#      if len(Swap_Error) != 0:
#          for a in Swap_Error["BGI_CORE_SYMBOL"]:
#              flash("{} has swap errors. Swap is null.".format(a))
#
#      # Minimising the data frame. Want to compare with median of last 15 days.
#      build_bgi_swap_df_show = build_bgi_swap_df[["BGI_CORE_SYMBOL", "LONG", "SHORT", "INSTI_LONG", "INSTI_SHORT"]]   # Data Frame
#
#      raw_result2 = db.engine.execute("SELECT * FROM test.bgi_swaps where date > CURDATE()-15")
#
#      result_data2 = raw_result2.fetchall()
#      result_col2 = raw_result2.keys()
#
#
#      cfd_swaps = pd.DataFrame(data=result_data2, columns=result_col2)
#      cfd_swaps.loc[:, "bgi_long"] = pd.to_numeric(cfd_swaps["bgi_long"])
#      cfd_swaps.loc[:, "bgi_short"] = pd.to_numeric(cfd_swaps["bgi_short"])
#      cfd_swaps.rename(columns={"Core_Symbol": "BGI_CORE_SYMBOL", "bgi_long":"BGI_LONG_AVERAGE", "bgi_short":"BGI_SHORT_AVERAGE"}, inplace=True)  # Rename for easy join.
#      cfd_swaps_median = cfd_swaps.groupby("BGI_CORE_SYMBOL").median()
#      combine_average_df = build_bgi_swap_df_show.join(cfd_swaps_median, on=["BGI_CORE_SYMBOL"])
#
#      # combine_average_df[]
#
#      # Data that we want to show.
#
#      # table_bgi_swaps_retail = combine_average_df.fillna("-").rename(columns=dict(zip(combine_average_df.columns,[a.replace("_","\n") for a in combine_average_df.columns]))).to_html(classes="table table-striped table-bordered table-hover table-condensed", index=False)
#      table_bgi_swaps_retail = combine_average_df.fillna("-").rename(columns=dict(zip(combine_average_df.columns,[a.replace("_","\n") for a in combine_average_df.columns]))).style.hide_index().applymap(color_negative_red, subset=["LONG","SHORT"]).set_table_attributes('class="table table-striped table-bordered table-hover table-condensed"').render()
#
#      table_bgi_swaps_insti = ""
#
#
#      table = combine_df.fillna("-").rename(columns=dict(zip(combine_df.columns,[a.replace("_","\n") for a in combine_df.columns]))).to_html(classes="table table-striped table-bordered table-hover table-condensed", index=False)
#
#      return render_template("upload_form2.html", table=table, form=form, table_bgi_swaps_retail=table_bgi_swaps_retail, table_bgi_swaps_insti=table_bgi_swaps_insti, uploaded_excel_data = uploaded_excel_data)
#
#  return render_template("upload_form.html", form=form)
#
