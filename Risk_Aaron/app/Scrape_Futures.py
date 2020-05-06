import requests
from bs4 import BeautifulSoup

import datetime
now = datetime.datetime.now()

from bs4 import BeautifulSoup

import logging

from Aaron_Lib import *
logging.basicConfig(level=logging.INFO)


# Can return a bool, or the actual float valu
def isfloat(str, value=False):
  try:
    str_float = float(str.replace(",",""))
    return str_float if value == True else True
  except ValueError:
    return str if value == True else  False


def get_capital_futures(url, column_thatis_alphanum = 5):

    try:
        r  = requests.get(url)      # Catch the return

        if r.status_code != 200:    # If request failed
            return {}

         # Get the text, and parse it into soup.
        soup = BeautifulSoup(r.text , 'html.parser')


        table = soup.find_all("table", recursive=True)
        if len(table) == 0:
            return {}

        # They have a table in a table.
        # We need to hunt for that.
        table_in_table_soup = [t for t in table if len(t.find_all("table")) > 0]

        in_brackets_re = re.compile(r'\((.*?)\)')
        exchange = ""   # Will need to find the exchange when it meets a tr with no table.

        # looking for table in a table. Will need to isolate the tr that has table in it.
        four_or_less = []   # For those unusable columns. Like.. Table headers.
        usable_data = []    # For those useable ones.

        for tab in table_in_table_soup:
            #for t in tab.children:
            tr_in_table = tab.find_all("tr", recursive=False)
            #print(len(tr_in_table))
            ##print("============================================\n\n")
            for tab_tr in tr_in_table:
                tab_tr_table = tab_tr.find_all("table", recursive=True)
                #print(len(tab_tr_table))
                if len(tab_tr_table) > 0:
                    # The data that we actually need..
                    data_tr = tab_tr.find_all("tr", recursive=True)
                    for dtr in data_tr:
                        data_td = dtr.find_all("td", recursive=True)

                        isalpha_counter = 0 # Will use to count how many alpha-numeric there are..

                        usable_data_buffer = [] # Will have to depend on the count to see if this will pass and get appended.
                        usable_data_buffer.append(exchange) # Need to append the exchange first.
                        for dtr in data_td:

                            if dtr.text.strip().isascii() and (dtr.text.strip().isalnum() or isfloat(dtr.text.strip())):
                                usable_data_buffer.append(isfloat(dtr.text.strip(),  value=True))  # We want to try and get the value if possible
                                isalpha_counter += 1

                        if isalpha_counter >= column_thatis_alphanum: # Has 5 or more. We can use this.
                            usable_data.append(usable_data_buffer)  # We accept this.
                        else:   # Those that don't make the cut. See if we need to troubleshoot. But mainly the chinese table header
                            #print(isalpha_counter)
                            #print(data_td)
                            four_or_less.append(data_td)
                else:
                    # Need to get the exchange.
                    tr_words_in_brackets = [i.group(1) for i in in_brackets_re.finditer(tab_tr.text)]
                    #print(tr_words_in_brackets)
                    if len(tr_words_in_brackets) > 0:
                        exchange = tr_words_in_brackets[0]  # We will take the first one..
                    else:
                        exchange = "Not Found"      # If the Exchange has not been found.


        return usable_data
    except:
        return []



def get_all_futures():



    # # Singapore Exchange
    url = "https://www.capitalfutures.com.tw/product/deposit_sp.asp?xy=2&xt=4"
    sg_exchange = get_capital_futures(url)

    # HK Exchange
    url = "https://www.capitalfutures.com.tw/product/deposit-hk.asp?xy=2&xt=5"
    hk_exchange = get_capital_futures(url)

    #US Exchange
    url = "https://www.capitalfutures.com.tw/product/deposit.asp?xy=2&xt=2"
    us_exchange = get_capital_futures(url)

    # Japan exchange. Alittle tricky cause it's a shared row..
    url = "https://www.capitalfutures.com.tw/product/deposit-jp.asp?xy=2&xt=3"
    # Shared rows has only 2-3 alphanumeric strings. Since it's chinese.
    jp_exchange = get_capital_futures(url, column_thatis_alphanum=4)

    # not dealing with the TW Stocks for now.. Theyhave no Symbol names
    url = "https://www.taifex.com.tw/cht/5/indexMarging"
    tw_exchange = get_capital_futures_TW(url)

    # Column that is needed.
    tw_col = ["Exchange", "Symbol", "Original Margin", "Maintenance_Margin"]
    col = ["Exchange", "Symbol", "Currency", "Original_Margin", "Maintenance_Margin"]

    exchanges = {"SG": [dict(zip(col, d)) for d in sg_exchange],
                 "HK": [dict(zip(col, d)) for d in hk_exchange],
                 "US": [dict(zip(col, d)) for d in us_exchange],
                 "JP": [dict(zip(col, d)) for d in jp_exchange],
                 "TW": [dict(zip(tw_col, d)) for d in tw_exchange]
                 }



    return exchanges



def get_capital_futures_TW(url, column_thatis_alphanum = 4):
    try:
        r  = requests.get(url)      # Catch the return

        if r.status_code != 200:    # If request failed
            return {}

         # Get the text, and parse it into soup.
        soup = BeautifulSoup(r.text , 'html.parser')


        table = soup.find_all("table", recursive=True)
        if len(table) == 0:
            return {}

        # Want to find all trs
        tr_in_table_soup = [t for t in table if len(t.find_all("tr")) > 0]

        exchange = "TW"   # Will need to find the exchange when it meets a tr with no table.

        # looking for table in a table. Will need to isolate the tr that has table in it.
        four_or_less = []   # For those unusable columns. Like.. Table headers.
        usable_data = []    # For those useable ones.

        for tab_tr in tr_in_table_soup:
            tr_tr_table = tab_tr.find_all("tr", recursive=True)
            #print(len(tab_tr_table))
            if len(tr_tr_table) > 0:
                #print(len(tr_tr_table))
                for dtr in tr_tr_table:
                    data_td = dtr.find_all("td", recursive=True)
                    if len(data_td) < 4:
                        continue
                    #print(len(data_td))
                    td_counter = 0 # Will use to count how many alpha-numeric there are..

                    usable_data_buffer = [] # Will have to depend on the count to see if this will pass and get appended.
                    usable_data_buffer.append(exchange) # Need to append the exchange first.
                    #print(dtr.text.strip())
                    for dtr in data_td:

                        usable_data_buffer.append(isfloat(dtr.text.strip(),  value=True))  # We want to try and get the value if possible
                        td_counter += 1

                    if td_counter >= column_thatis_alphanum: # Has 'column_thatis_alphanum' or more. We can use this.
                        usable_data.append(usable_data_buffer)  # We accept this.
                    else:   # Those that don't make the cut. See if we need to troubleshoot. But mainly the chinese table header
                        #print(isalpha_counter)
                        #print(data_td)
                        four_or_less.append(data_td)



            tw_Wanted = {r'臺股期貨' : "TX", r'小型臺指' : "Mtx"}
            # Extract out those that we need.
            return_data = [u for u in usable_data if any([k in u for k,d in tw_Wanted.items()])]
            # Change the Chinese Symbols to BGI related ones.
            return_data = [ [d if d not in tw_Wanted else tw_Wanted[d] for d in r] for r in return_data]
            # Want Only the 1st, 3rd and 4th column
            return_data = [d[0:2] + d[3:] for d in return_data]

        return return_data
    except:
        return []

# To generate the column and insert params
# Takes in an array of dict
def generate_sql_insert(data):

    col = ["Exchange", "Symbol", "Currency", "Original_Margin", "Maintenance_Margin"]

    # SQl column has _ instead of spaces
    sql_columns = ", ".join("`{}`".format(d) for d in [c.replace(" ","_") for c in col] + ["Date_Time"])

    sql_data = []
    for d in data:
        data_buffer = []
        for c in col:
            if c in d:
                data_buffer.append("'{}'".format(d[c]))
            else:
                data_buffer.append("NULL")
        # To append for date_time
        data_buffer.append("NOW()")


        sql_data.append("({})".format(", ".join(data_buffer)))

    sql_col = "({})".format(sql_columns)

    return (sql_col, sql_data)



def Get_previous_Future_data(db):
    sql_query = """SELECT Exchange, Symbol, Original_Margin, Maintenance_Margin, Date_Time 
    FROM `future_contract_sizes` as A
    WHERE Date_Time = (
        SELECT max(Date_Time) 
        FROM `future_contract_sizes` as B 
        WHERE B.Exchange = A.Exchange and B.Symbol = A.Symbol
        )
    """
    raw_result = db.engine.execute(sql_query)
    result_data = raw_result.fetchall()     # Return Result
    # dict of the results
    result_col = raw_result.keys()
    # Clean up the data. Date.
    result_data_clean = [[a.strftime("%Y-%m-%d %H:%M:%S") if isinstance(a, datetime.datetime) else a for a in d] for d in result_data]
    return result_col, result_data_clean


# Putting it into Dict with Tuple as keys. To easily verify
def futures_into_dict_tuple(data):
    past_margin_data = {}
    for d in data:
        if all([c in d for c in ["Exchange", "Symbol", "Original_Margin", "Maintenance_Margin"]]):
            if "Date_Time" in d:    # If Date_time is available, we want it. Else, no need.
                past_margin_data[(d["Exchange"], d["Symbol"])] = [
                d["Original_Margin"], d["Maintenance_Margin"], d["Date_Time"]]
            else:
                past_margin_data[(d["Exchange"], d["Symbol"])] = [
                    d["Original_Margin"], d["Maintenance_Margin"]]
    return past_margin_data

# Compare the past and now data,
# Return keys of the difference
def compare_past_now(Current_margin_dict, past_margin_dict):

    # Compare
    difference_key = []
    for k,d in Current_margin_dict.items():
        if k not in past_margin_dict:
            pass
        elif [str(dd) for dd in d] != [past_margin_dict[k][0], past_margin_dict[k][1]]:
            difference_key.append(k)
            #print (k,d)
    return difference_key

# Want a way to determine if DB has been connected or not.
def Get_Current_Futures_Margin(db=False, sendemail=True):
    # Current Data
    return_val = get_all_futures()
    consolidated_data = [data_dict for k,d in return_val.items() for data_dict in d]
    sql_col, sql_data = generate_sql_insert(consolidated_data)  # Will put the column and data in the correct places
    consolidated_sql_statement = sql_multiple_insert(tablename = "aaron.future_contract_sizes", column = sql_col, values = sql_data, footer = "", sql_max_insert = 500)
    Current_margin_dict = futures_into_dict_tuple(consolidated_data)

    # get connection to db
    if db == False:
        db = init_SQLALCHEMY()

    # Past Data
    result_col, result_data_clean = Get_previous_Future_data(db)
    past_margin = [dict(zip(result_col,r)) for r in result_data_clean]
    past_margin_dict = futures_into_dict_tuple(past_margin)

    # Compare for the difference.
    #[k for k in Current_margin_dict if k not in past_margin_dict]
    difference_key = compare_past_now(Current_margin_dict=Current_margin_dict, past_margin_dict=past_margin_dict)

    if sendemail == True:
        if len(difference_key) > 0: # There are differences found. we want to send out a notification.
            array_of_difference = [list(k) + past_margin_dict[k] + Current_margin_dict[k] for k in difference_key]
            # The columns that are needed, in order, to be placed into HTML table.
            table_header = ["Exchange", "Symbol", "Original Margin (Past)", "Maintenance Margin (Past)", "Date_Time (Past)", "Original Margin", "Maintenance Margin"]
            html_table = Array_To_HTML_Table(Table_Header=table_header, Table_Data=array_of_difference)

            email_body = "{Email_Header}Hi,<br><br>There were some changes to the Futures Margins on https://www.taifex.com <br>Kindly see the difference in the table below<br> \
                    {html_table}<br>The Futures margin excel can be downloaded here: <a href='{url}'>Download Future Excel.</a> \
                    <br><br>Thanks,<br>Aaron{Email_Footer}".format(Email_Header=Email_Header, html_table=html_table, url="http://202.88.105.3:5000/Futures/Scrape",Email_Footer=Email_Footer)

            #EMAIL_LIST_ALERT
            Send_Email(To_recipients=["aaron.lim@blackwellglobal.com"], cc_recipients=[], Subject="Futures Margin Changed",
                             HTML_Text=email_body, Attachment_Name=[])
        else:   # If there are no changes.
            email_body = "{Email_Header}Hi,<br><br>Futures Margins on https://www.taifex.com Remains unchanged.<br><br> \
            Excel file can be downloaded at: <a href='{url}'>Download Future Excel.</a><br><br>Thanks,<br>Aaron{Email_Footer}".format(
                Email_Header=Email_Header, url="http://202.88.105.3:5000/Futures/Scrape",Email_Footer=Email_Footer)


            #EMAIL_LIST_ALERT
            Send_Email(To_recipients=["aaron.lim@blackwellglobal.com"], cc_recipients=[], Subject="Futures Margin Checked",
                             HTML_Text=email_body, Attachment_Name=[])

        # Put the latest details into SQL
        # Will only run when we use the scheduler.
        # Will NOT run when using Flask.
        for c in consolidated_sql_statement:
            sql_trades_insert = text(c)  # To make it to SQL friendly text.
            raw_insert_result = db.engine.execute(sql_trades_insert)

    return return_val

# If we are not importing this, we want to run main()
if __name__ == '__main__':
    Get_Current_Futures_Margin()