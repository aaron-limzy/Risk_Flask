import requests
from bs4 import BeautifulSoup

import datetime
now = datetime.datetime.now()

from io import StringIO
import json
from Aaron_Lib import *
from bs4 import BeautifulSoup

from sqlalchemy import create_engine, text

import pandas as pd
import numpy as np



#From https://www.fxdd.com/mt/en/trading/offering
#FXdd Pricing is from https://www.fxdd.com/api/price-feed/clear as Json
#Price_URL_Clear = "https://www.fxdd.com/api/price-feed/clear"
#Price_URL_Standard = "https://www.fxdd.com/api/price-feed/standard"




def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False


def get_swaps_fxdd():
    #https://www.fxdd.com/mt/en/trading/offering
    try:
        #url = "http://www.fxdd.com/fileadmin/setup/libraries/proxy/jason.php"   #The URL for their JSON
        url = "https://secure.fxdd.com/fileadmin/setup/libraries/proxy/jason.php"   # The URL that return JSON. Changed endpoint on 21 Nov 2019

        r  = requests.get(url)

        if r.status_code != 200:    # If request failed
            return {}

        data = r.text       #Get the text form
        Fxdd_Swap_Json_String = StringIO(r.text)
        fxdd_Swap_Json = json.load(Fxdd_Swap_Json_String)


        if len(fxdd_Swap_Json) < 2: # If length is less than 2
            return []

        # Want to get the index of "buy" and "sell" programmably in case they swap it around.
        long_index = max([i if fxdd_Swap_Json[0][i].lower().find("buy")>= 0 else -1 for i in range(len(fxdd_Swap_Json[0]))])
        short_index = max([i if fxdd_Swap_Json[0][i].lower().find("sell") >= 0 else -1 for i in range(len(fxdd_Swap_Json[0]))])
        symbol_index =  max([i if fxdd_Swap_Json[0][i].lower().find("cp") >= 0 else -1 for i in range(len(fxdd_Swap_Json[0]))])


        if long_index == -1 or short_index == -1 or symbol_index == -1: # Return error.
            return []

        # Want to get the long and short in float, IF it is, else, don't want it.
        # also, to replace "/"
        return_list_dict = [{"Symbol": s[symbol_index].replace("/",""), "Long" : float(s[long_index]), "Short": float(s[short_index])}
                           for s in fxdd_Swap_Json[1:] if isfloat(s[long_index]) and isfloat(s[short_index])]



        # for s in fxdd_Swap_Json[1:]:    # Since the first one is the column names
        #     if isfloat(s[long_index]) and isfloat(s[short_index]):
        #         multiplier = 100000     # Not given in points. Need to multiply by digits
        #         if s[symbol_index][-3:].find("JPY") >= 0:
        #             multiplier = 1000
        #         elif s[symbol_index][0].find("X") >= 0:
        #             multiplier = 100
        #
        #
        #         return_list_dict.append({"Symbol": s[symbol_index].replace("/",""),
        #                                  "Long" : -1*multiplier*float(s[long_index]), "Short": multiplier*float(s[short_index])})

        return return_list_dict
    except:
        return []



def get_swaps_forexDotCom():

    # Information got off https://www.forex.com/en/trading/pricing-fees/rollover-rates/

    # # The Original payload header.
    # payload = {"siteId": "forex.web.g2en", "products": [], "marketType": "FX", "productType": "FX",
    #  "requiredFields": ["Product", "Long", "Short", "AliasName", "Product", "SEOName", "MarketId",
    #                     "HasMultipleProductTypes"]}

    # Forex.com (Rollover rates displayed are based on a 10K position)
    # We need 1 lot. So, multiply by 10
    multiplier = 10

    # Has CFD. But we will not do the conversion as we it dosn't show on the website.

    # CHN.50
    # USA.30
    # ESP.35
    # FRA.40
    # GER.30
    # HKG.33
    # JPN225
    # NAS100
    # OILUSD
    # SPX500
    # UK.100

    try:
        # We took off the fields that we do not need
        payload = {"siteId": "forex.web.g2en", "products": [], "marketType": "FX", "productType": "FX",
         "requiredFields": ["Product", "Long", "Short"]}


        url = "https://www.forex.com/_Srvc/feeds/LiveRates.asmx/GetMostPopularMarkets"

        return_data = requests.post(url, json=payload)

        if return_data.status_code != 200:
            print("Something went wrong with the Post request to forex.com while getting swaps.")
            return []

        swaps = return_data.json()

        if 'd' not in swaps:    # Usually swaps would be in swaps['d'] when it is returned from forex.com
            print("Something went wrong with the Post request to forex.com while getting swaps. No 'd' field")
            return []


        return_list_dict = []
        for d in swaps['d']:

            # Need to check if all the keys are in.
            if all([k in d for k in ["Product", "Long", "Short"]]) and isfloat( d['Long']) and isfloat(d['Short']):
                product = d['Product'] if 'Product' in d else ""
                long = round(multiplier * d['Long'], 2) if 'Long' in d else 0         # Need to multiply to our contract size, since fdc does 10K sizes
                short = round(multiplier* d['Short'],2) if 'Short' in d else 0       # Need to multiply to our contract size, since fdc does 10K sizes
                swap_val_dict = {"Symbol": product.replace("/",""), "Long" : float(long), "Short": float(short)}
                return_list_dict.append(swap_val_dict)
        return return_list_dict
    except:
        return []

# Need to correct for digits
# Need to check if today isn't available.
# It's usually 1 day late.
def get_swaps_saxo():

    try:
        #https://www.home.saxo/en-sg/rates-and-conditions/forex/trading-conditions#historic-swap-points

        payload = {"date": "{}".format(get_working_day_date(datetime.date.today(), -1, 1).strftime("%Y-%m-%d"))}
        # If they don't have today's, need to check for yesterday, or the lsat working day


        return_data = requests.post("https://www.home.saxo/en-SG/rnc/hsp", json=payload)
        swaps = return_data.json()

        # Hardcode the dict keys as per json return.
        col_names = [j['name'] for j in swaps['rncHistoricSwapPoints']['tableData']['head'][1]['cells'] ]
        col_names[0] = "Symbol"  # Need to hard code since their table format is different.


        swap_val = [ [i['cells'][j]['name']  for j in range(len(i['cells']))] for i in swaps['rncHistoricSwapPoints']['tableData']['body']]

        swap_list_dict = [dict(zip(col_names, s)) for s in swap_val]

        # We don't need the added information. Will only use the Long and Short
        swap_long_short = []

        #[{"Symbol": s['Symbol'], "Long": float(s["Long positions"]), "Short": float(s["Short positions"])}
        #                   for s in swap_list_dict if isfloat(s["Long positions"]) and isfloat(s["Short positions"])]


        for s in swap_list_dict:
            if isfloat(s["Long positions"]) and isfloat(s["Short positions"]):
                multiplier = 100000  # Not given in points. Need to multiply by digits
                if s['Symbol'][-3:].find("JPY") >= 0:
                    multiplier = 1000
                elif s['Symbol'][0].find("X") >= 0:
                    multiplier = 100

                swap_long_short.append({"Symbol": s['Symbol'], "Long": -1*multiplier*float(s["Long positions"]), "Short":multiplier*float(s["Short positions"])})





        return swap_long_short

    except:
        return []

# Scrape if off the website.
# Not by JSON
def get_swaps_tradeview():
    try:
        url = "https://www.tradeviewforex.com/room/forex-resources/rollover-rates"

        return_data = requests.get(url=url)
        soup = BeautifulSoup(return_data.text, 'html.parser')
        table = soup.find_all("table")
        if len(table) == 0:
            return {}

        tradeview_to_bgi_symbols = {'WS30':'.US30', 'UK100': '.UK100', 'SPXm' : '.US500',  'NDX' :'.US100',
             'FCHI' :'.F40',  'GDAXI' : '.DE30',
             'STOXX50E' : '.STOXX50', 'J225' : '.JP225','AUS200': '.AUS200', 'HSI': '.HK50'}
        # Get the header from the table
        col_names = [th.text for th in table[0].tr.find_all('th')]

        swap_long_short = []
        for tr in table[0].find_all("tr"):
            swap_data = [float(td.text) if isfloat(td.text) else td.text for td in tr.find_all('td')]

            #print(swap_data)
            if len(swap_data) == len(col_names):
                if swap_data[0] in tradeview_to_bgi_symbols:    # Want to do the CFD conversion
                    swap_data[0] = tradeview_to_bgi_symbols[swap_data[0]]

                swap_long_short.append(dict(zip(col_names, swap_data)))
        return swap_long_short
    except:
        return {}

# Scrape if off the website.
# Not by JSON
def get_swaps_fpmarkets():
    try:
        url = "https://www.fpmarkets.com/swap-point"
        return_data = requests.get(url=url)
        soup = BeautifulSoup(return_data.text, 'html.parser')
        table = soup.find_all("table")
        if len(table) == 0:
            return {}


        fpm_to_BGI_symbol = {'GER30' : '.DE30',   'FRA40': '.F40', 'UK100' : '.UK100', 'JP225':'.JP225',
                             'US30' : ".US30",  'US100' : '.US100',
                                'US500' : '.US500',  'XTIUSD' : '.USOil',  'XBRUSD' : '.UKOil' ,
                                "HK50" : ".HK50",
                                "EURO50" : ".STOXX50",
                                "CHINA50" : ".A50"}

        # Get the header from the table
        col_names = [th.text for th in table[0].tr.find_all('th')]

        swap_long_short = []
        for tr in table[0].find_all("tr"):
            swap_data = [float(td.text) if isfloat(td.text) else td.text for td in tr.find_all('td')]
            #print(swap_data)
            if len(swap_data) == len(col_names):
                if swap_data[0] in fpm_to_BGI_symbol: # Doing CFD Conversion
                    swap_data[0] =  fpm_to_BGI_symbol[swap_data[0]]
                swap_long_short.append(dict(zip(col_names, swap_data)))
        return swap_long_short
    except:
        return {}

# Scrape if off the website.
# Not by JSON
def get_swaps_ebhforex():
    try:
        url="https://ebhforex.com/faq/rollover-policy/"
        return_data = requests.get(url=url)
        soup = BeautifulSoup(return_data.text, 'html.parser')
        table = soup.find_all("table")
        if len(table) == 0:
            return {}

        ebh_to_BGI_symbol = {'D30EUR.' : '.DE30',   'F40EUR.': '.F40', '100GBP.' : '.UK100', '225JPY.':'.JP225',
                             'U30USD.' : ".US30",  'NASUSD.' : '.US100',
                                'SPXUSD.' : '.US500',  'USOUSD.' : '.USOil',  'UKOUSD.' : '.UKOil' }



        tr = table[0].find_all('tr')    # Want to get to the 2nd row of the table.
        # Get the header from the table
        col_names_1 = [th.text for th in tr[0]]
        col_names_2 = [th.text for th in tr[1]]

        swap_long_short = []
        for tr_1 in tr[2:]: # Want to start from the 3rd row.

            col_name_table = [td.get_attribute_list('class')[0] for td in tr_1.find_all('td')]
            long_index_list = [i if isinstance(col_name_table[i],str) and col_name_table[i].lower().find("long")>=0 else -1 for i in range(len(col_name_table))]
            long_index = max(long_index_list)
            short_index_list = [i if isinstance(col_name_table[i],str) and col_name_table[i].lower().find("short")>=0 else -1 for i in range(len(col_name_table))]
            short_index = max(short_index_list)

            if long_index == -1 or short_index == -1:
                continue

            # # Example of swap data.
            # ['AUDUSD.', -1.7535, -5.6805, '', '', '']
            # ['NZDUSD.', -5.124, -1.764, '', '', '']
            # ['USDCAD.', -2.163, -9.093, '', '', '']
            # ['AUDCHF.', 9.6615, -22.785, '', '', '']
            # ['EURAUD.', -65.646, 22.135, '', '', '']
            swap_data = [float(td.text) if isfloat(td.text) else td.text for td in tr_1.find_all('td')]
            #print(swap_data)

            if len(swap_data) >= 3: # Want to do symbol conversion for CFDs
                if swap_data[0] in ebh_to_BGI_symbol:
                    swap_data[0] = ebh_to_BGI_symbol[swap_data[0]]
                else:   # Their own FX Symbol has a . we want to remove that.
                   swap_data[0] = swap_data[0].replace(".","")


            # Need to build the symbol Dict. Need to remove . from the symbol name.
            swap_long_short.append(dict(zip(["Symbol" ,"Long", "Short"], [swap_data[0], swap_data[long_index],swap_data[short_index]])))

        return swap_long_short
    except:
        return {}





## https://www.globalprime.com/trading-conditions/swaps-financing/
##https://admin.gleneagle.com.au/Account/SwapPoints
# Not by JSON
def get_swaps_globalprime():
    try:
        url = "https://admin.gleneagle.com.au/Account/SwapPoints"
        return_data = requests.get(url=url)
        soup = BeautifulSoup(return_data.text, 'html.parser')
        table = soup.find_all("table")
        if len(table) == 0:
            return {}

        # Symbol Translations.
        globalprime_to_BGI_Symbol = {'AUS200': '.AUS200', 'EUSTX50': '.STOXX50', 'FRA40': '.F40', 'GER30': '.DE30',
                                     'HK50': '.HK50',
                                     'JPN225': '.JP225', 'NAS100': '.US100', 'UK100': '.UK100', 'UKOIL': '.UKOil',
                                     'US30': '.US30',
                                     'US500': '.US500', 'XTIUSD': '.USOil'}
        swap_long_short = []
        for t in table:
            # Get the header from the table
            col_names = [th.text for th in t.tr.find_all('th')]

            # Want to dynamically find Long and Short Index
            long_index_list = [i if isinstance(col_names[i], str) and col_names[i].lower().find("long") >= 0 else -1 for i in range(len(col_names))]
            long_index = max(long_index_list)
            short_index_list = [i if isinstance(col_names[i], str) and col_names[i].lower().find("short") >= 0 else -1 for i in range(len(col_names))]
            short_index = max(short_index_list)

            if long_index == -1 or short_index == -1:   # If cannot determine if it's a short or long index
                continue


            for tr in t.find_all("tr"):
                swap_data = [float(td.text) if isfloat(td.text) else td.text for td in tr.find_all('td')]
                #print(swap_data)
                if len(swap_data) == len(col_names):
                    # Want to do the symbol translation.
                    # Mainly for CFDs tho.
                    if swap_data[0] in globalprime_to_BGI_Symbol:
                        swap_data[0] = globalprime_to_BGI_Symbol[swap_data[0]]
                    else:
                        swap_data[0] = swap_data[0].replace(".", "")
                    swap_long_short.append(dict(zip(["Symbol", "Long", "Short"],[swap_data[0], swap_data[long_index], swap_data[short_index]])))


        return swap_long_short
    except:
        return {}


# Want to use the same code as Flask.
# Will use sqlalchemy instead of py-sql
def init_SQLALCHEMY():
    #TODO: Link this with Flask's config. Else, there  might be trouble when moving SQL Bases.
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/aaron'
    return create_engine(SQLALCHEMY_DATABASE_URI)

# Will attempt to get from SQL (64.73). (if possible), and save it into a file.
# If not possible, will read from the file. Fail-safe should SQL not be connected as well.
def get_from_sql_or_file(sql_query_line, file_name, db):
            # """SELECT Core_Symbol, Contract_size, Digits, Type, Currency, m.Swap_markup_profile as Swap_markup_profile, Long_Markup, Short_Markup
            # FROM aaron.swap_bgicoresymbol as b, aaron.swap_markup_profile as m
            # where b.swap_markup_profile = m.Swap_markup_profile"""


    try:    # Will try to connect to SQL
        #db = init_SQLALCHEMY()
        sql_query = text(sql_query_line)
        raw_result = db.engine.execute(sql_query)               # Query the SQL
        result_data = raw_result.fetchall()                     # Return Result
        result_col = raw_result.keys()                          # Dict of the results
        result = pd.DataFrame(result_data, columns=result_col)  # Form into Dataframe

        # file_name = "Upload_Swaps/BGI_core_symbol.xls"
        # Want to save it, with no index.
        result.to_excel(file_name, index=False)
        return result
    except: # If cannot, we will read the file and return the results from that excel.
        return pd.read_excel(file_name)


# Will get other broker swaps with requests.
# and join them together.
def get_broker_swaps():
    df_fxdd = pd.DataFrame(get_swaps_fxdd())
    # df_fxdd['Long'] = df_fxdd['Long'].apply(lambda x: round(x, 2) if type(x) == "float" or  type(x) == "int" else x)
    # df_fxdd['Short'] = df_fxdd['Long'].apply(lambda x: round(x, 2) if type(x) == "float" or type(x) == "int" else x)
    df_fxdd = df_fxdd.rename(columns={"Long": "fxdd_Long", "Short": "fxdd_Short"})

    df_fdc = pd.DataFrame(get_swaps_forexDotCom())
    #df_fdc['Long'] = df_fdc['Long'].apply(lambda x: round(x, 2) if type(x) == "float" or  type(x) == "int" else x)
    #df_fdc['Short'] = df_fdc['Short'].apply(lambda x: round(x, 2) if type(x) == "float" or type(x) == "int" else x)
    df_fdc = df_fdc.rename(columns={"Long": "fdc_Long", "Short": "fdc_Short"})

    df_saxo = pd.DataFrame(get_swaps_saxo())
    df_saxo = df_saxo.rename(columns={"Long": "saxo_Long", "Short": "saxo_Short"})

    df_tradeview = pd.DataFrame(get_swaps_tradeview())
    df_tradeview = df_tradeview.rename(columns={"Long": "tv_Long", "Short": "tv_Short"})

    df_global_prime = pd.DataFrame(get_swaps_globalprime())
    df_global_prime = df_global_prime.rename(columns={"Long": "gp_Long", "Short": "gp_Short"})

    df_ebhforex = pd.DataFrame(get_swaps_ebhforex())
    df_ebhforex = df_ebhforex.rename(columns={"Long": "ebh_Long", "Short": "ebh_Short"})

    df_fpmarkets = pd.DataFrame(get_swaps_fpmarkets())
    df_fpmarkets = df_fpmarkets.rename(columns={"Long": "fpm_Long", "Short": "fpm_Short"})

    swaps_array = [df_fxdd, df_fdc, df_saxo, df_tradeview, df_global_prime, df_ebhforex, df_fpmarkets]
    df_return = pd.DataFrame([], columns=["Symbol"])
    how = "outer"
    for s in swaps_array:
        if "Symbol" in s and len(s) > 0:
            df_return = df_return.merge(s, on="Symbol", how=how)
    return df_return