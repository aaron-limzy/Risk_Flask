
import json
import datetime


# Version 1.2  - To be compatible with OZ new REST API. Version 85 as well as back version 75.
#              - Changes include the ability to check if  Settings["CoreSymbolFilter"]["CoreSymbol"] exists.
#              - It will auto detect between the 2 and produce the needed excel files.
#

import ctypes  # An included library with Python install.

import requests
#from Aaron_Lib_Risk import *
from Aaron_Lib import *


#OZ_Margin_Account = 35

# self.REST_Password = "Rest123"
# self.REST_Login = "aaronlim_margin"
# self.BRIDGE_IP = "38.76.4.235:44300"         # Live Margin Bridge

#

class OZ_Rest_Class:

    def __init__(self, hub):
        print("hub:{}".format(hub))
        self.Bridge_REST_Version = "1.06"
        self.BRIDGE_IP = ""
        self.REST_Password = ""
        self.REST_Login = ""

        if hub.lower() == "margin":
            self.REST_Password = "Rest123"
            self.REST_Login = "aaronlim_margin"
            self.BRIDGE_IP = "38.76.4.235:44300"         # Live Margin Bridge
        elif hub.lower() == "retail" :
            self.BRIDGE_IP = "38.76.4.235:44301"  # Live Retail Bridge
            self.REST_Password = "riskrisk"
            self.REST_Login = "Risk_API"

        self.ACCESS_CODE = self.Get_AccessCode()

    # Get the access code from OZ bridge
    def Get_AccessCode(self):
        payload = {'grant_type': 'password',"username" : self.REST_Login, "password":self.REST_Password, "rest_version":self.Bridge_REST_Version}
        Post_Return = requests.post('https://' + self.BRIDGE_IP + '/api/token', verify = False, data=payload)

        if(Post_Return.status_code == 200):
            print("Return code okay. ")
        else:
            print("Something went wrong with the Access code request")
            return "ERROR: " + Post_Return.text

        Return_Json = Post_Return.json()

        if("access_token" in Return_Json):  # If the access code isn't there.
            return Return_Json["access_token"]
        return "ERROR: " + Post_Return.text


    def post_rest_w_Idempotency(self, url, payload):


        #'Content-type': 'application/json','Accept': 'text/plain'
        Authorization = {"Authorization": "Bearer " + self.ACCESS_CODE, 'Idempotency-Key': "{}".format(datetime.datetime.now().timestamp())}

        #payload = {"amount": "100", "type":"CREDIT", "comment":"BGI Credit in"}
        #payload={}
        Post_Return = requests.post(url='https://' + self.BRIDGE_IP + '/api/rest/{url}'.format(url=url), verify=False,
                                    data=json.dumps(payload) ,headers=Authorization)
        return Post_Return
        print(Post_Return.text)

    # Will CREDIT/CASH into the Margin Account
    # Will return a tuple of  (True/False, Json)
    # Return Access Code for use later.
    def balance_in_OZ_margin(self, margin_account, amount, b_c_type, comment=""):

        Access_code = self.ACCESS_CODE                          # Get the access code from OZ

        url = "margin-account/{id}/ledger".format(id=margin_account)

        # Building the payload
        payload = {"amount": "{}".format(amount), "type" : "{}".format(b_c_type), "comment": "{}".format(comment)}

        Post_Return = self.post_rest_w_Idempotency(url, payload)

        return (Post_Return.status_code == 200,  Post_Return.json())



    def Rest_Get_OZ(self, endpoint):

        # if("ERROR" in Access_code) :   #There has been an error.
        #     return Access_code
        #     #return -1
        #     1==1

        Access_code = self.ACCESS_CODE

        #The request Header
        Authorization = {"Authorization" : "Bearer " + Access_code}
        #Get_Return = requests.get( "https://" + self.BRIDGE_IP + "/api/rest/bridge-settings/SymbolSettings", verify=False, headers = Authorization)
        print("https://" + self.BRIDGE_IP + "/api/rest/{endpoint}".format(endpoint=endpoint))
        Get_Return = requests.get("https://" + self.BRIDGE_IP + "/api/rest/{endpoint}".format(endpoint=endpoint), verify=False, headers=Authorization)

        return Get_Return


    # Returns
    # [
    # {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/21', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/21/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/21/'}, 'writeLedger': {'href': 'margin-account/21/ledger', 'requiresWrite': True}}, 'id': 21, 'name': 'Squared', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': [], 'comment': '', 'balance': '0', 'accountCredit': '0', 'unrealizedPnL': '0', 'equity': '0', 'margin': '0', 'freeMargin': '0', 'marginUtilizationPercentage': '0', 'autoLiquidationEnabled': True, 'autoLiquidationPercentage': '150', 'marginCallPercentage': '110'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/22', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/22/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/22/'}, 'writeLedger': {'href': 'margin-account/22/ledger', 'requiresWrite': True}}, 'id': 22, 'name': 'Vantage', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': [], 'comment': '', 'balance': '-1570439.11', 'accountCredit': '3200000', 'unrealizedPnL': '5291.7', 'equity': '1634852.59', 'margin': '90949.1', 'freeMargin': '1543903.49', 'marginUtilizationPercentage': '5.563', 'autoLiquidationEnabled': False, 'marginCallPercentage': '110'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/28', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/28/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/28/'}, 'takers': {'href': 'margin-account/28/takers', 'requiresWrite': False}, 'trade': {'href': 'margin-account/28/trade', 'requiresWrite': True}, 'writeLedger': {'href': 'margin-account/28/ledger', 'requiresWrite': True}}, 'id': 28, 'name': 'RetailMT4-1-Guo', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': ['RetailMT4-1-Guo'], 'comment': '', 'balance': '5897856.45', 'accountCredit': '508000', 'unrealizedPnL': '0', 'equity': '6405856.45', 'margin': '0', 'freeMargin': '6405856.45', 'marginUtilizationPercentage': '0', 'autoLiquidationEnabled': False, 'marginCallPercentage': '125'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/30', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/30/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/30/'}, 'takers': {'href': 'margin-account/30/takers', 'requiresWrite': False}, 'trade': {'href': 'margin-account/30/trade', 'requiresWrite': True}, 'writeLedger': {'href': 'margin-account/30/ledger', 'requiresWrite': True}}, 'id': 30, 'name': 'V-Vantage', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': ['RB-Vantage'], 'comment': '', 'balance': '14483816.46', 'accountCredit': '0', 'unrealizedPnL': '-316052.25', 'equity': '14167764.21', 'margin': '0', 'freeMargin': '14167764.21', 'marginUtilizationPercentage': '0', 'autoLiquidationEnabled': False, 'marginCallPercentage': '125'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/33', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/33/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/33/'}, 'takers': {'href': 'margin-account/33/takers', 'requiresWrite': False}, 'trade': {'href': 'margin-account/33/trade', 'requiresWrite': True}, 'writeLedger': {'href': 'margin-account/33/ledger', 'requiresWrite': True}}, 'id': 33, 'name': 'V-CMC', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': ['RB-CMC'], 'comment': '', 'balance': '996206565.87', 'accountCredit': '100', 'unrealizedPnL': '-191819.88', 'equity': '996014845.99', 'margin': '0', 'freeMargin': '996014845.99', 'marginUtilizationPercentage': '0', 'autoLiquidationEnabled': False, 'marginCallPercentage': '120'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/34', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/34/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/34/'}, 'writeLedger': {'href': 'margin-account/34/ledger', 'requiresWrite': True}}, 'id': 34, 'name': 'CMC', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': [], 'comment': '', 'balance': '279152.56', 'accountCredit': '0', 'unrealizedPnL': '0', 'equity': '279152.56', 'margin': '0', 'freeMargin': '279152.56', 'marginUtilizationPercentage': '0', 'autoLiquidationEnabled': False, 'marginCallPercentage': '100'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/35', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/35/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/35/'}, 'takers': {'href': 'margin-account/35/takers', 'requiresWrite': False}, 'trade': {'href': 'margin-account/35/trade', 'requiresWrite': True}, 'writeLedger': {'href': 'margin-account/35/ledger', 'requiresWrite': True}}, 'id': 35, 'name': '1002_ATG', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': ['1002_ATG'], 'comment': '', 'balance': '56439.16', 'accountCredit': '0', 'unrealizedPnL': '158.83', 'equity': '56597.99', 'margin': '2746.64', 'freeMargin': '53851.35', 'marginUtilizationPercentage': '4.853', 'autoLiquidationEnabled': True, 'autoLiquidationPercentage': '140', 'marginCallPercentage': '100'}, {'_links': {'brokerView': {'href': 'urn:mvc/controller/broker-view/36', 'requiresWrite': False}, 'readLedger': {'href': 'margin-account/36/ledger', 'requiresWrite': False}, 'self': {'href': 'margin-account/36/'}, 'takers': {'href': 'margin-account/36/takers', 'requiresWrite': False}, 'trade': {'href': 'margin-account/36/trade', 'requiresWrite': True}, 'writeLedger': {'href': 'margin-account/36/ledger', 'requiresWrite': True}}, 'id': 36, 'name': '1003_PPDE', 'currency': 'USD', 'equityPrecisionDecimalPlaces': 2, 'reverseMakerPositions': False, 'takerIds': ['PPDE'], 'comment': '', 'balance': '0', 'accountCredit': '0', 'unrealizedPnL': '0', 'equity': '0', 'margin': '0', 'freeMargin': '0', 'marginUtilizationPercentage': '0', 'autoLiquidationEnabled': True, 'autoLiquidationPercentage': '140', 'marginCallPercentage': '100'}]
    def Get_OZ_margin_accounts(self):


        #Access_code = access_code if len(access_code) > 2 else self.Get_AccessCode()

        if ("ERROR" in self.ACCESS_CODE):  # There has been an error.
            return "Error in Access Code from OZ."
        margin_account_str = self.Rest_Get_OZ(endpoint="margin-account")
        margin_account_data = margin_account_str.json()
        #return return_text
        return margin_account_data


        #Helper Function
        # Print the margin dict to string


    def oz_margin_to_str(self, m):

        if all([k in m for k in ["id", 'name', 'currency', 'balance', 'accountCredit', 'unrealizedPnL', 'margin',
                                 'marginUtilizationPercentage']]):
            id = m['id']
            name = m['name']
            currency = m['currency']
            balance = float(m['balance']) if isfloat(m['balance']) else m['balance']
            account_credit = float(m['accountCredit']) if isfloat(m['accountCredit']) else m['accountCredit']
            floating_PnL = float(m['unrealizedPnL']) if isfloat(m['unrealizedPnL']) else m['unrealizedPnL']
            margin = m['margin']
            margin_percent = m['marginUtilizationPercentage']

            #print("balance: {}, type: {}".format(balance, type(balance)))

            return "Id: {id}\n  Name: <b>{name}</b>\n  Balance: {balance} {currency} \n  Credit: {credit} {currency}\n  Floating_PnL: {floating_PnL} {currency}\n  Margin: {margin} {currency}\n  Margin_percent: {margin_percent} % \n\n".format(
            currency=currency,
                id=id, name=name,
                balance="{:,.2f}".format(balance) if isinstance(balance, float) else balance,
                credit="{:,.2f}".format(account_credit) if isinstance(account_credit, float) else account_credit,
                floating_PnL="{:,.2f}".format(floating_PnL) if isinstance(floating_PnL, float) else floating_PnL,
                margin=margin,
                margin_percent=margin_percent)

        else:
            return "{}".format(m)


    #Get the string from OZ Bridge.
    def get_string(self, Buffer):

        if isinstance(Buffer, dict):
            for i in Buffer:
                self.get_string(Buffer[i])
                #print(Buffer.keys())
        else:
            print(Buffer)




    def get_margin_acc_position(self, id):

        position_reply = self.Rest_Get_OZ("/margin-account/{id}/positions".format(id=id))
        position_json = position_reply.json()

        if 'data' in position_json:
            return position_json['data']
        else:
            return position_json['message']


    def margin_acc_to_string(self,account_position):

        #return_str = "<pre>"
        return_list = []
        return_list.append(" <b>_Symbol</b>_|  _<b>Position</b>_ | <b>Float PnL(USD)</b>")
        #return_str += " <b>_Symbol</b> | <b>Float_pnl(USD)</b> |  <b>Position</b>  | <b>Margin</b> \n"
        #return_str += " Symbol | Float_pnl |  Position  | Margin |\n"
        #print(" Symbol | Float_pnl |  Position |")
        for d in account_position:
            coresymbol = d['coreSymbol']
            floating_pnl = "${:,.2f}".format(float(d['unrealizedPnL'])) if isfloat(d['unrealizedPnL']) else d['unrealizedPnL']

            if isfloat(d['position']):
                position_float = int(d['position']) # Want to get the int. So we can decide how to best represent it. (Thousands(k) or not k)
                position = "{:,} k".format(int(int(d['position'])/1000)) if position_float % 1000 == 0 else "{:,}".format(int(d['position']))
            else:
                position = d['position']


            #margin = d['margin']

            # buf = "<pre>{coresymbol:^8}|{floating_pnl:^11}|{position:^12}|{margin:^8}</pre>\n".format(
            #     coresymbol=coresymbol, floating_pnl=floating_pnl, position=position, margin=margin)


            buf = "<pre>{coresymbol:^8}|{position:^10}|{floating_pnl:^11}</pre>".format(
                coresymbol=coresymbol, floating_pnl=floating_pnl, position=position)
            return_list.append(buf)


            #print(buf)
        #return_str = "</pre>"
        return return_list

    # Get the Symbol Markups and return it as a dict
    def get_symbol_settings_rules(self):

        price_channel_rule_req = self.Rest_Get_OZ("/settings/price-channel-rule")




        #Checks for Status code return.
        if price_channel_rule_req.status_code != 200:
            print("Error: Return from Setting Rules Give Request error {}".format(price_channel_rule_req.status_code))
            return

        price_channel_rule = price_channel_rule_req.json()

        return price_channel_rule

    # Want to get core Symbol setting.
    def get_core_symbol(self):
        price_channel_rule_req = self.Rest_Get_OZ("hub-settings/SymbolSettings")

        if price_channel_rule_req.status_code != 200:
            print("Error: Return from Setting Rules Give Request error {}".format(price_channel_rule_req.status_code))
            return

        symbol_setting_all = price_channel_rule_req.json()


        return symbol_setting_all



    # # Get the settings from OZ as data
    # def Get_Margin_Setting(Access_code):
    #
    #     # if("ERROR" in Access_code) :   #There has been an error.
    #     #     #return -1
    #     #     1==1
    #     #The request Header
    #     Authorization = {"Authorization" : "Bearer " + Access_code}
    #     payload = { "marginProfile": '1002_ATG'}
    #     #payload = {"marginProfile": ''}
    #     # Sending Margin profile to the margin profile. Margin Account 35
    #     patch_margin_account = requests.patch(url='https://{self.BRIDGE_IP}/api/rest/settings/margin-adapter-account/{id}'.format(self.BRIDGE_IP=self.BRIDGE_IP,id=OZ_Margin_Account),verify = False, data=json.dumps(payload), headers=Authorization)
    #
    #
    #     if patch_margin_account.status_code == 200:
    #         return (1, "Successful")
    #     else:
    #         return (-1, patch_margin_account.json())

    #
    #
    # # Get the settings from OZ as data
    # def Get_Settings(self, Access_code=""):
    #
    #     if Access_code == "":
    #         Access_code = self.Get_AccessCode()
    #
    #     if("ERROR" in Access_code) :   #There has been an error.
    #         #return -1
    #         1==1
    #     #The request Header
    #     Authorization = {"Authorization" : "Bearer " + Access_code}
    #     #Get_Return = requests.get( "https://" + self.BRIDGE_IP + "/api/rest/bridge-settings/SymbolSettings", verify=False, headers = Authorization)
    #     Get_Return = requests.get("https://" + self.BRIDGE_IP + "/api/rest/settings/margin-adapter-account", verify=False, headers=Authorization)
    #
    #     if(Get_Return.status_code == 200):
    #         1==1
    #     else:
    #         1==1
    #         #return "ERROR: Get error. " + Get_Return.text
    #
    #     print(Get_Return.text)
    #     JSON_Get_Return = Get_Return.json()
    #
    #
    #     if ("settings" in JSON_Get_Return.keys()):
    #         if ("SettingsRules" in JSON_Get_Return["settings"].keys()):
    #             print("Got the Settings.")
    #             return JSON_Get_Return["settings"]["SettingsRules"]
    #     return "ERROR: No settings found. "
    #
    # # Flatten the received JSON into [{}]
    # # So that it is easier to write into Excel.
    # def Read_each_Line(self, Current_Line):
    #
    #     Line = []
    #     for i in range(len(Current_Line)):
    #
    #         Line_Buffer = {}
    #         Error_Found = 0
    #
    #         if ("CoreSymbol" in Current_Line[i].keys()):
    #             Line_Buffer["CoreSymbol"] = Current_Line[i]["CoreSymbol"]
    #         elif (("CoreSymbolFilter" in Current_Line[i].keys()) and ("CoreSymbol" in Current_Line[i]["CoreSymbolFilter"].keys())):
    #             Line_Buffer["CoreSymbol"] = Current_Line[i]["CoreSymbolFilter"]["CoreSymbol"]       #Append the Symbol
    #
    #             if ("SymbolGroupPath" in Current_Line[i]["CoreSymbolFilter"].keys()):
    #                 Line_Buffer["SymbolGroupPath"] = Current_Line[i]["CoreSymbolFilter"]["SymbolGroupPath"]  # Append the SymbolGroupPath
    #
    #         else:
    #             Error_Found = Error_Found + 1
    #
    #         if ("PriceChannelID" in Current_Line[i].keys()):
    #             Line_Buffer["PriceChannelID"] = Current_Line[i]["PriceChannelID"]
    #         else:
    #             Error_Found = Error_Found + 1
    #
    #         if ("Settings" in Current_Line[i].keys()):
    #             Setting_Key_Array = Current_Line[i]["Settings"].keys()
    #
    #             if ("FeedToTakers" in Setting_Key_Array):
    #                 Line_Buffer["FeedToTakers"] = str(Current_Line[i]["Settings"]["FeedToTakers"])
    #
    #             if ("BidSpreadPoints" in Setting_Key_Array):
    #                 Line_Buffer["BidSpreadPoints"] = str(Current_Line[i]["Settings"]["BidSpreadPoints"])
    #
    #             if ("AskSpreadPoints" in Setting_Key_Array):
    #                 Line_Buffer["AskSpreadPoints"] = str(Current_Line[i]["Settings"]["AskSpreadPoints"])
    #
    #             if ("MinSpreadPoints" in Setting_Key_Array):
    #                 Line_Buffer["MinSpreadPoints"] = str(Current_Line[i]["Settings"]["MinSpreadPoints"])
    #             else:
    #                 Line_Buffer["MinSpreadPoints"] = ""
    #
    #             if ("MaxSpreadPoints" in Setting_Key_Array):
    #                 Line_Buffer["MaxSpreadPoints"] = str(Current_Line[i]["Settings"]["MaxSpreadPoints"])
    #             else:
    #                 Line_Buffer["MaxSpreadPoints"] = ""
    #
    #             Line_Buffer["MaxSpreadGapLevelPointsForSTP"] = ""  # Not in the Excel Sheet.
    #
    #             if ("Type" in Setting_Key_Array):
    #                 Line_Buffer["Type"] = str(Current_Line[i]["Settings"]["Type"])
    #
    #             if ("PriceReportingSettings" in Setting_Key_Array):
    #                 PriceReportingSettings_Keys = Current_Line[i]["Settings"]["PriceReportingSettings"].keys()
    #
    #                 if ("MarketOrderPriceReporting" in PriceReportingSettings_Keys):
    #                     Line_Buffer["MarketOrderPriceReporting"] = str(
    #                         Current_Line[i]["Settings"]["PriceReportingSettings"]["MarketOrderPriceReporting"])
    #
    #                 if ("InstantOrderPriceReporting" in PriceReportingSettings_Keys):
    #                     Line_Buffer["InstantOrderPriceReporting"] = str(
    #                         Current_Line[i]["Settings"]["PriceReportingSettings"]["InstantOrderPriceReporting"])
    #
    #                 if ("LimitOrderPriceReporting" in PriceReportingSettings_Keys):
    #                     Line_Buffer["LimitOrderPriceReporting"] = str(
    #                         Current_Line[i]["Settings"]["PriceReportingSettings"]["LimitOrderPriceReporting"])
    #
    #                 if ("StopOrderPriceReporting" in PriceReportingSettings_Keys):
    #                     Line_Buffer["StopOrderPriceReporting"] = str(
    #                         Current_Line[i]["Settings"]["PriceReportingSettings"]["StopOrderPriceReporting"])
    #
    #                 if ("PreviouslyQuotedPriceReporting" in PriceReportingSettings_Keys):
    #                     Line_Buffer["PreviouslyQuotedPriceReporting"] = str(
    #                         Current_Line[i]["Settings"]["PriceReportingSettings"]["PreviouslyQuotedPriceReporting"])
    #
    #                 if ("ExchangeOrderPriceReporting" in PriceReportingSettings_Keys):
    #                     Line_Buffer["ExchangeOrderPriceReporting"] = str(
    #                         Current_Line[i]["Settings"]["PriceReportingSettings"]["ExchangeOrderPriceReporting"])
    #
    #             if ("MakerIds" in Setting_Key_Array):   #Might be a list. Need some special care.
    #                 if isinstance(Current_Line[i]["Settings"]["MakerIds"],list):
    #                     Comma_Count = 0;
    #                     String_Buffer = '"['
    #
    #                     for j in Current_Line[i]["Settings"]["MakerIds"]:
    #
    #                         #To add in comma when needed.
    #                         if (Comma_Count == 0):
    #                             Comma_Count = Comma_Count + 1
    #                         else:
    #                             String_Buffer = String_Buffer + ','
    #                         String_Buffer = String_Buffer + '""' + str(j) + '""'
    #
    #                     String_Buffer = String_Buffer + ']"'
    #                     Line_Buffer["MakerIds"] = String_Buffer
    #
    #             Line.append(Line_Buffer)
    #         else:
    #             print("Error Found")
    #             Error_Found = Error_Found + 1
    #
    #     if(Error_Found > 0):
    #         ctypes.windll.user32.MessageBoxW(0, "Error: Can't flatten the array. ", "Translation error.", 1)
    #         return []   #Return an empty array.
    #     else:
    #         print("Flaterning of array done.");
    #         return Line
    #
    # #Need to rename each column to fit OZ style.
    # def Rename_Column(self, Array_Data):
    #
    #     Name_Change_Dict={
    #         "CoreSymbol"                    :   "Core Symbol",
    #         "SymbolGroupPath"               :   "Symbol Group Path",
    #         "PriceChannelID"                : "Price Channel ID",
    #         "FeedToTakers"                  : "Feed To Takers",
    #         "BidSpreadPoints"               : "Bid Spread Points",
    #         "AskSpreadPoints"               : "Ask Spread Points",
    #         "MinSpreadPoints"               : "Min Spread Points",
    #         "MaxSpreadPoints"               : "Max Spread Points",
    #         "MaxSpreadGapLevelPointsForSTP" : "Max Spread Gap Level Points For STP",
    #         "Type"                          : "Type",
    #         "MarketOrderPriceReporting"     : "Market Order Price Reporting",
    #         "InstantOrderPriceReporting"    : "Instant Order Price Reporting",
    #         "LimitOrderPriceReporting"      : "Limit Order Price Reporting",
    #         "StopOrderPriceReporting"       : "Stop Order Price Reporting",
    #         "PreviouslyQuotedPriceReporting": "Previously Quoted Price Reporting",
    #         "ExchangeOrderPriceReporting"   : "Exchange Order Price Reporting",
    #         "MakerIds"                      : "Maker Ids",
    #
    #         }
    #
    #     Return_Array = [];
    #     for i in range(len(Array_Data)):
    #         Buffer_Dict = {}
    #         for j in Array_Data[i]:
    #             for k in Name_Change_Dict:
    #                 if (j is k):
    #                     Buffer_Dict[Name_Change_Dict[k]] = Array_Data[i][j]
    #                     break
    #         Return_Array.append(Buffer_Dict)
    #
    #     return Return_Array
    #
    #

    #
    #
    # #Searches for the symbols with spread to change, and change them.
    # # Change_Index
    # # 0 = Mon/Fri 8am,
    # # 1 = Tues/Friday 4am,
    # # 2 = Sat 4am
    # def Search_And_Edit(self, Symbol_Setting_Data, Change_Index):
    #
    #     To_Change_Data = [
    #         {'CoreSymbol': 'XAG/USD', 'PriceChannelID': '.hk_Live1',
    #          'Settings': [  {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 40, 'MaxSpreadPoints': 40},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 60, 'MaxSpreadPoints': 60},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1']
    #          },
    #         {'CoreSymbol': 'XAU/USD', 'PriceChannelID': '.hk_Live1',
    #          'Settings': [  {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 50, 'MaxSpreadPoints': 50},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1']
    #          },
    #         {'CoreSymbol': 'XAG/USD', 'PriceChannelID': '.hkk_Live1',
    #          'Settings': [  {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 40, 'MaxSpreadPoints': 40},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 50, 'MaxSpreadPoints': 50},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1']
    #          },
    #         {'CoreSymbol': 'XAU/USD', 'PriceChannelID': '.hkk_Live1',
    #          'Settings': [{'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 40, 'MaxSpreadPoints': 40},
    #                      {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 50, 'MaxSpreadPoints': 50},
    #                      {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1']
    #          },
    #         {'CoreSymbol': 'XAU/USD', 'PriceChannelID': '`_Live1',
    #          'Settings': [  {'BidSpreadPoints': -3, 'AskSpreadPoints': 3, 'MinSpreadPoints': "", 'MaxSpreadPoints': ""},
    #                         {'BidSpreadPoints': -8, 'AskSpreadPoints': 8, 'MinSpreadPoints': "", 'MaxSpreadPoints': ""},
    #                         {'BidSpreadPoints': -8, 'AskSpreadPoints': 8, 'MinSpreadPoints': "", 'MaxSpreadPoints': ""}],
    #          'MakerIds': ['MarginMT4-1']
    #          },
    #
    #
    #         ## Guo Stuff-------------------------------------------------------------------------------------------------
    #         {'CoreSymbol': 'XAG/USD', 'PriceChannelID': '.hk_Live1-Guo',
    #          'Settings':[   {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 40, 'MaxSpreadPoints': 40},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 60, 'MaxSpreadPoints': 60},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1-Guo']
    #          },
    #         {'CoreSymbol': 'XAU/USD', 'PriceChannelID': '.hk_Live1-Guo',
    #          'Settings': [{'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 50, 'MaxSpreadPoints': 50},
    #                      {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80},
    #                      {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1-Guo']
    #          },
    #         {'CoreSymbol': 'XAG/USD', 'PriceChannelID': '.hkk_Live1-Guo',
    #          'Settings': [ {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 40, 'MaxSpreadPoints': 40},
    #                        {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 50, 'MaxSpreadPoints': 50},
    #                        {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1-Guo']
    #          },
    #         {'CoreSymbol': 'XAU/USD', 'PriceChannelID': '.hkk_Live1-Guo',
    #          'Settings': [  {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 40, 'MaxSpreadPoints': 40},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 50, 'MaxSpreadPoints': 50},
    #                         {'BidSpreadPoints': 0, 'AskSpreadPoints': 0, 'MinSpreadPoints': 80, 'MaxSpreadPoints': 80}],
    #          'MakerIds': ['MarginMT4-1-Guo']
    #          },
    #         {'CoreSymbol': 'XAU/USD', 'PriceChannelID': '`_Live1-Guo',
    #          'Settings': [  {'BidSpreadPoints': -3, 'AskSpreadPoints': 3, 'MinSpreadPoints': "", 'MaxSpreadPoints': ""},
    #                         {'BidSpreadPoints': -8, 'AskSpreadPoints': 8, 'MinSpreadPoints': "", 'MaxSpreadPoints': ""},
    #                         {'BidSpreadPoints': -8, 'AskSpreadPoints': 8, 'MinSpreadPoints': "", 'MaxSpreadPoints': ""}],
    #          'MakerIds': ['MarginMT4-1-Guo']
    #          }
    #     ]
    #
    #
    #     for j in range(len(To_Change_Data)):
    #         for i in range(len(Symbol_Setting_Data)):               # Loop thru
    #             if(isinstance(Symbol_Setting_Data[i],dict)):         # For each data.
    #
    #                 CoreSymbol_Match = 0
    #                 if("CoreSymbol" in Symbol_Setting_Data[i]):     # For previous version. Version 75
    #                     if(Symbol_Setting_Data[i]["CoreSymbol"] == To_Change_Data[j]['CoreSymbol']):
    #                         CoreSymbol_Match = 1
    #                 elif( ("CoreSymbolFilter" in Symbol_Setting_Data[i]) and ("CoreSymbol" in Symbol_Setting_Data[i]["CoreSymbolFilter"]) ):     #For version 85
    #                     if(Symbol_Setting_Data[i]["CoreSymbolFilter"]["CoreSymbol"] == To_Change_Data[j]['CoreSymbol']):
    #                         CoreSymbol_Match = 1
    #
    #
    #
    #                 if (CoreSymbol_Match == 1 and
    #                      "PriceChannelID" in Symbol_Setting_Data[i] and Symbol_Setting_Data[i]["PriceChannelID"] ==  To_Change_Data[j]['PriceChannelID'] and
    #                         "MakerIds" in Symbol_Setting_Data[i]["Settings"] and Symbol_Setting_Data[i]["Settings"]["MakerIds"] == To_Change_Data[j]['MakerIds']):    # Found the symbol
    #
    #                     for k in To_Change_Data[j]['Settings'][Change_Index]:   # Change the settings.
    #                         Symbol_Setting_Data[i]["Settings"][k] = To_Change_Data[j]['Settings'][Change_Index][k]
    #                     break
    #
    #
    #
    #     return Symbol_Setting_Data
    #                 #print(Symbol_Setting_Data[i]["CoreSymbol"] + "  :   " + Symbol_Setting_Data[i]["Settings"]["MakerIds"][0])
    # #Price Channel ID
    #



# Writing an array or Dict to a csv file.
# Need a [{}]
def Write_Array_To_File(Array_Data, File_Name):

    file = open(File_Name, "w")

    if(isinstance(Array_Data, list) and len(Array_Data) > 0 and isinstance(Array_Data[0], dict)):
        Comma_Counter = 0;
        for Keys in Array_Data[0].keys():

            #Comma Correction
            if Comma_Counter == 0:
                Comma_Counter = Comma_Counter +  1
            else:
                file.write(",")


            file.write(Keys)
            #print(Keys)
        file.write("\n")


    for i in range(len(Array_Data)):
        Comma_Counter = 0
        for j in Array_Data[i]:
            #Comma Correction
            if Comma_Counter == 0:
                Comma_Counter = Comma_Counter +  1
            else:
                file.write(",")

            file.write(Array_Data[i][j])
        file.write("\n")

    file.close()

# Create the Folder.
def Create_Folder():
    now = datetime.datetime.now()   # Read the system time.
    Dir_String = "OZ_Spread_Change(Daily)_" + now.strftime("%d%b%Y")
    if(Dir_String not in os.listdir(".")): #Folder exists.
        os.mkdir(Dir_String)
    return Dir_String


# a = OZ_Rest_Class("retail")
# print(a.BRIDGE_IP)
# print(a.Get_OZ_margin_accounts())
# a.balance_in_OZ_margin(21, -1, "CREDIT", "")
