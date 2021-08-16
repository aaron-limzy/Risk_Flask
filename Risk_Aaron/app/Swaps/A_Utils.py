
from Aaron_Lib import *
from Helper_Flask_Lib import *
import numpy as np

# Get the dividend value from the list of dicts.
def find_dividend(data, symbol, date):

    for d in data:
        if "mt4_symbol" in d and d["mt4_symbol"] == symbol and "date" in d and d["date"] == date:
            return_val = ""
            if (get_working_day_date(start_date=date, weekdays_count=-1) == datetime.date.today()):
                return_val += "Today"   # Want to append words, for javascript to know which to highlight
            if d["dividend"] == 0:
                return "{}-".format(return_val)
            else:
                return "{}{}".format(d["dividend"], return_val)
    else:
        return "X"


# [{'Core_Symbol': '.A50', 'bgi_long': '0.000000000000', 'bgi_short': '0.000000000000', 'Date': datetime.date(2019, 8, 13)},....]
def find_swaps(data, symbol, date, Long_Short):
    for d in data:
        if 'Core_Symbol' in d and d['Core_Symbol'] == symbol:
            if 'Date' in d and d['Date'] == date:
                if isinstance(d[Long_Short], float) or isinstance(d[Long_Short], int) or Check_Float(d[Long_Short]):
                    return "{:.4f}".format(float(d[Long_Short]))
                else:
                    return d[Long_Short]
    return 'X'


# To check if the file exists. If it does, generate a new name.
def Check_File_Exist(path, filename):
    folders = [a for a in path.split("/") if (a != "" and a != ".")]
    for i, folder in enumerate(folders):
        folder = "/".join(folders[:i + 1])  # WE Want the current one as well.
        if os.path.isdir(folder) == False:
            os.mkdir(folder)
    File_Counter = 0
    FileName = filename  # To be used and changed later. Appended to the end.
    while True:
        full_path_filename = path + "/" + FileName
        if os.path.isfile(full_path_filename) == False:
            break
        else:
            File_Counter += 1
            FileName_Split = filename.split(".")
            FileName_Split[-2] = FileName_Split[-2] + "_" + str(File_Counter)
            FileName = ".".join(FileName_Split)

    return path + "/" + FileName

# SF wrote this function.
# To craft the SQL statement for the update of symbol spread.
def HK_Change_Spread_SQL(df, database):
    draft01 = """WHEN '{x2}' THEN {x1} """
    list01 = df['fixed']
    list02 = df['postfixsymb']
    #
    # print(list02)
    # print(",".join(["'{}'".format(s) for s in list02]))

    temp = []
    for a, b in zip(list01, list02):
        temp.append(draft01.format(x1=a, x2=b))
    draft02 = "".join(temp)

    Update_query = text("""
    Update """ + database + """.`symbol_o`
    SET fixedspread = CASE postfixsymb
    """ + draft02 + """ END
    WHERE symbol_o.postfixsymb IN ({})
    """.format(",".join(["'{}'".format(s) for s in list02])))

    return Update_query



# Want to get the spread from the MT5_Futures Database
def get_HK_Spread(symbol_list):

    sql_query = """SELECT * FROM risk.`symbol_o`
     WHERE postfixsymb in ({}) 
     ORDER BY postfixsymb""".format(",".join(["'{}'".format(s) for s in symbol_list]))

    data = Query_Symbol_Markup_db_engine(sql_query)
    df = pd.DataFrame(data)
    print(df)

    return df

def get_HKG_spread(test=False):
    # # HKG details are from C.
    hkg_prog_name = "Lab_HKG_SpreadChange.exe" if test == True else "Live1_HKG_SpreadChange.exe"
    C_Return_Val, output, err = Run_C_Prog(
        "app" + url_for('static',
                        filename='Exec/HK_Change_Spread/{}'.format(hkg_prog_name)) + " Check")
    return C_Return_Val

def combine_spread_sql_hkg(symbol_list, test):

    start_time = datetime.datetime.now()
    df_spread = get_HK_Spread(symbol_list)
    hkg_spread = get_HKG_spread(test=test)

    df_return = df_spread[["postfixsymb", "fixedspread", "digits"]]


    # Hard code the details of HKG in.
    df_return=df_return.append({'postfixsymb': 'HKG',
                      'fixedspread': hkg_spread,
                                "digits" : 0},
                     ignore_index=True)

    df_return["spread_dollar"] =  df_return["fixedspread"] / (10 ** df_return["digits"])

    print("Time taken: {}".format((datetime.datetime.now() - start_time).total_seconds()))

    return df_return