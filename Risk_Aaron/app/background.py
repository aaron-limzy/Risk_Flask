# For the background for some of the websites.


def background_pic(website):

    # Need to find bg for:
    # BGI_MT5_Symbol_Float


    #NoTrade_Change_ReadOnly_Settings


    default = "css/Mac_Coffee.jpg"
    gb_return = {"save_BGI_float" :         "css/city_overview.jpg",
                 "BGI_Country_Float":       "css/World_Map.jpg",
                 "BGI_Symbol_Float" :       "css/leaves.png",
                 "symbol_float_trades" :    "css/double-bubble.png",
                 "Country_float_trades" :   "css/leaves_2.png",
                 "group_float_trades"  :    "css/webb.png",
                 "symbol_closed_trades" :   "css/double-bubble.png",
                 "Client_Comment_Scalp" :   "css/runner_1.jpg",
                 "Client_trades_form" :     "css/cactus.jpg",
                 "Client_trades_Analysis" : "css/strips_1.png",
                 "BGI_MT5_Symbol_Float":    "css/geometry.png",

                 "USOil_Ticks" :                        "css/Oil_Rig_2.jpg",
                 "CFH_Soap_Position" :                  "css/notebook_pen.jpg",
                 "CFH_Symbol_Update" :                  "css/notebook_pen.jpg",
                 "Changed_readonly" :                   "css/Mac_Coffee.jpg",
                 "Monitor_Risk_Tools" :                 "css/clock_left.jpg",
                 "Computer_Usage" :                     "css/mac_keyboard_side.jpg",
                 "BGI_Convert_Rate" :                   "css/autumn.jpg",
                 "Scrape_futures" :                     "css/Color-Pencil.jpg",
                 "Monitor_Account_Trades_Settings" :    "css/Glasses_3.jpeg",
                 "Monitor_Account_Trades" :             "css/Glasses_3.jpeg",
                 "ABook_Matching" :                     "css/tic-tac-toe.png",

                 "save_mt5_BGI_float":                  "css/city1.jpg",        # MT5 Stuff.

                 "Risk_auto_cut" :                      'css/Scissors.jpg',
                 "noopentrades_changegroup":            "css/Faded_car.jpg",

                  "Futures_LP_Details":                 "css/giftly.png",
                  "HK_Copy_STP":                        "css/leaves-pattern.png",
                 "ABook_BGI" :                           "css/sativa.png"
        }

    #background_pic("Monitor_Account_Trades")

    # Christmas series
    # "css/Christmas_vector_1.jpg"
    # "css/Christmas_vector_13.jpg"
    # "css/Christmas_vector_1.jpg"

    # Background for CNY
    # "ABook_Matching": "css/cny4.jpg",
    #"Client_trades_Analysis": "css/cny10.jpg",
    # "symbol_float_trades": "css/cny9.jpg",
    # "BGI_Symbol_Float": "css/cny5.png",
    # "symbol_closed_trades": "css/cny9.jpg",
    return_val = gb_return[website] if website in gb_return else default


    return return_val


def icon_pic(website):


    default = "css/icon/thinking.png"
    gb_return = {"ABook_BGI" :                           "css/icon/MickeyMouseIcon.png",
                 "ABook_Matching":                                   "css/icon/bank.png",
                 "Risk_auto_cut":                               "css/icon/cut.png",

                 }

    #background_pic("Monitor_Account_Trades")

    # Christmas series
    # "css/Christmas_vector_1.jpg"
    # "css/Christmas_vector_13.jpg"
    # "css/Christmas_vector_1.jpg"

    # Background for CNY
    # "ABook_Matching": "css/cny4.jpg",
    #"Client_trades_Analysis": "css/cny10.jpg",
    # "symbol_float_trades": "css/cny9.jpg",
    # "BGI_Symbol_Float": "css/cny5.png",
    # "symbol_closed_trades": "css/cny9.jpg",
    return_val = gb_return[website] if website in gb_return else default


    return return_val