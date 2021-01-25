import asyncio
from enum import Enum
import logging
import random
from aiopyfix.connection import ConnectionState, MessageDirection
from client_connection import FIXClient
from aiopyfix.engine import FIXEngine
from aiopyfix.message import FIXMessage
from sqlalchemy import create_engine, text
from time import sleep

import datetime
import logging
logging.basicConfig(level=logging.INFO)

class Side(Enum):
    buy = 1
    sell = 2

# Will get connected to CFH FIX
# Will Send FIx Message to ask for position
# Will then send to ask for Account Info (Balance Credit...)
# Will then log out. And Close off loop.
class Client(FIXEngine):
    def __init__(self):
        #FIXEngine.__init__(self, "TS2LiveFix8.cfixtech.com")

        # addr= "3018.DemoFixGW.com"
        # targetCompId = "CfhDemoOrders"
        # senderCompId = "BGI_NZ_DataDemo"
        # userName = "BGI_NZ_DataDemo"
        # password = "6SPwaVqJ"
        # self.client_num = "197201"

        self.client_account_info = dict()
        self.client_open_position = dict()


        #self.loop = loop # Place holder

        #self.loop= asyncio.get_event_loop()
        # Live environment.
        addr= "TS2LiveFix8.cfixtech.com"
        targetCompId = "CfhLiveOrders"
        senderCompId = "BGI_NZ_Data"
        userName = "BGI_NZ_Data"
        password = "NghaK4jZ"
        self.client_num = "27840"

        self.db = create_engine( 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/aaron')

        FIXEngine.__init__(self, addr)
        self.clOrdID = 0
        self.msgGenerator = None

        # create a FIX Client using the FIX 4.4 standard
        self.client = FIXClient(self, protocol="pyfix.FIX44", targetCompId=targetCompId, senderCompId=senderCompId,
                                userName=userName, password=password, heartbeat=5)



        # we register some listeners since we want to know when the connection goes up or down
        self.client.addConnectionListener(self.onConnect, ConnectionState.CONNECTED)
        self.client.addConnectionListener(self.onDisconnect, ConnectionState.DISCONNECTED)

    async def start(self, host, port, loop):
        self.loop = loop
        print("Start")
        await self.client.start(host, port, loop)

        #self.client.session.session.resetMsgs()

    async def onConnect(self, session):
        logging.info("Established connection to %s" % (session.address(),))
        # register to receive message notifications on the session which has just been created
        session.addMessageHandler(self.onLogin, MessageDirection.INBOUND, self.client.protocol.msgtype.LOGON)
        session.addMessageHandler(self.onLogout, MessageDirection.INBOUND, self.client.protocol.msgtype.LOGOUT)


        #session.addMessageHandler(self.onExecutionReport, MessageDirection.INBOUND, self.client.protocol.msgtype.EXECUTIONREPORT)
        session.addMessageHandler(self.heartbeat, MessageDirection.INBOUND, self.client.protocol.msgtype.HEARTBEAT)
        session.addMessageHandler(self.account_info, MessageDirection.INBOUND, "AAB")   # For incoming Account Info.
        session.addMessageHandler(self.position_info, MessageDirection.INBOUND, self.client.protocol.msgtype.POSITIONREPORT)  # For incoming Account Info.
    #
    # async def hearbeat(self, connectionHandler):
    #     logging.info("Heartbeat")


    async def onDisconnect(self, session):
        logging.info("%s has disconnected" % (session.address(),))
        # we need to clean up our handlers, since this session is disconnected now
        session.removeMessageHandler(self.onLogin, MessageDirection.INBOUND, self.client.protocol.msgtype.LOGON)
        # session.removeMessageHandler(self.onExecutionReport, MessageDirection.INBOUND,
        #                              self.client.protocol.msgtype.EXECUTIONREPORT)

        logging.info("Stopping all loops.")
        self.loop.stop()

    async def position_info(self, connectionHandler, msg):   # If it replies with the account Info.
        logging.info("account position recieved: {}".format(msg))
        codec = connectionHandler.codec

        # Need to clear the dict to prepare for new information
        # self.client_open_position = dict()

        #print(type(msg))
        if codec.protocol.fixtags.Symbol in msg:
            symbol = msg.getField(codec.protocol.fixtags.Symbol)

            if codec.protocol.fixtags.NoPositions in msg:
                # NoPosition = msg.getField(codec.protocol.fixtags.NoPositions)
                # print(NoPosition)
                # print(type(NoPosition))

                if msg.isRepeatingGroup(codec.protocol.fixtags.NoPositions):    # if it's a repeating Group
                    repeat_group = msg.getRepeatingGroupByTag(codec.protocol.fixtags.NoPositions, codec.protocol.fixtags.PosType,
                                                     "DLV")
                    if codec.protocol.fixtags.LongQty in repeat_group:
                            #print(repeat_group.getField(codec.protocol.fixtags.LongQty))
                        self.client_open_position[symbol] = float(repeat_group.getField(codec.protocol.fixtags.LongQty))
                    if codec.protocol.fixtags.ShortQty in repeat_group:
                            #print(repeat_group.getField(codec.protocol.fixtags.ShortQty))
                        self.client_open_position[symbol] = -1 * float(repeat_group.getField(codec.protocol.fixtags.ShortQty))


    async def account_info(self, connectionHandler, msg):   # If it replies with the account Info.
        logging.info("account_info recieved: {}".format(msg))



        if "5020" in msg:
            balance = msg.getField("5020")
            #logging.info("Balance: {}".format(balance))
            self.client_account_info["Balance"] = balance
        if "5021" in msg:
            AvailableForMarginTrading = msg.getField("5021")
            #logging.info("AvailableForMarginTrading: {}".format(AvailableForMarginTrading))
            self.client_account_info["AvailableForMarginTrading"] = AvailableForMarginTrading
        if "5023" in msg:
            SecurityDeposit = msg.getField("5023")
            #logging.info("SecurityDeposit: {}".format(msg.getField("5023")))
            self.client_account_info["SecurityDeposit"] = SecurityDeposit
        if "5024" in msg:
            ClosedPL = msg.getField("5024")
            #logging.info("ClosedPL: {}".format(msg.getField("5024")))
            self.client_account_info["ClosedPL"] = ClosedPL
        if "5025" in msg:
            OpenPL = msg.getField("5025")
            #logging.info("OpenPL: {}".format(msg.getField("5025")))
            self.client_account_info["OpenPL"] = OpenPL
        if "5026" in msg:
            MarginRequirement = msg.getField("5026")
            #logging.info("MarginRequirement: {}".format(msg.getField("5026")))
            self.client_account_info["MarginRequirement"] = MarginRequirement
        if "5027" in msg:
            NetOpenPosition = msg.getField("5027")
            #logging.info("NetOpenPosition: {}".format(msg.getField("5027")))
            self.client_account_info["NetOpenPosition"] = NetOpenPosition

        #print(self.client_account_info)
        #print(self.client_open_position)

        #await self.account_logout(connectionHandler)


        #connectionHandler.handle_close()
        #self.loop.stop()  # kill the Loop.

        # for m in msg.tags:
        #     print(m)


    # Function to update the SQL position from the CFH FIX.
    # CFH_Position = {"EURUSD": 100000, "GBPUSD": 2300, ...}
    async def fix_position_sql_update(self):

        CFH_Position = self.client_open_position

        #print("len of CFH_Position: {}".format(len(CFH_Position)))

        # First, we want to update the position, as well as the updated time.
        fix_position_database = "aaron"
        fix_position_table = "cfh_live_position_fix"

        # Want to construct the statement for the insert into the  DB.table.
        # For the values that are non-zero
        fix_position_header = """INSERT INTO {fix_position_database}.{fix_position_table} (`Symbol`, `position`, `Updated_time`) VALUES """.format(
            fix_position_database=fix_position_database, fix_position_table=fix_position_table)

        fix_position_values = ["('{}', '{}', now()) ".format(k, d) for k, d in CFH_Position.items()]
        fix_position_footer = """ ON DUPLICATE KEY UPDATE position=VALUES(position), Updated_time=VALUES(Updated_time)"""

        if len(CFH_Position) >0:
            print("{} {} {}".format(fix_position_header, " , ".join(fix_position_values), fix_position_footer))
            self.db.engine.execute(text("{} {} {}".format(fix_position_header, " , ".join(fix_position_values), fix_position_footer)))  # Insert into DB

        # Async update SQL to save runtime
        # async_sql_insert(app=current_app._get_current_object(), header=fix_position_header,
        #                  values=fix_position_values, footer=fix_position_footer)

        if len(CFH_Position) == 0:
            CFH_Position[""] = ""

        # Want to Update to Zero, for those position that are not opened now.
        Update_to_zero = """UPDATE {fix_position_database}.{fix_position_table} set position = 0, Updated_time = now() where Symbol not in ({open_symbol})""".format(
            fix_position_database=fix_position_database, fix_position_table=fix_position_table,
            open_symbol=" , ".join(['"{}"'.format(k) for k in CFH_Position]))

        print(Update_to_zero)
        self.db.engine.execute(Update_to_zero)  # Insert into DB

        # Async update SQL. No header and footer as we will construct the whole statement here.
        # async_sql_insert(app=current_app._get_current_object(), header="", values=[Update_to_zero], footer="")

        return

    async def chf_fix_details_ajax(self, update_tool_time=1):  # Return the Bloomberg dividend table in Json.

        datetime_now = datetime.datetime.utcnow()
        #datetime_now.weekday()  # 0 - monday

        # if cfh_fix_timing() == False:  # Want to check if CFH Fix still running.
        #     return_data = [[{"Comment": "Out of CFH Fix timing. From UTC Sunday 2215 to Friday 2215"}],
        #                    [{"Comment": "Out of CFH Fix timing. From UTC Sunday 2215 to Friday 2215"}]]
        #     return json.dumps(return_data)

        # Get the Position and Info from CFH FIX.
        account_info = self.client_account_info
        #[account_info, account_position] = CFH_Position_n_Info()

        if len(account_info) == 0:  # If there are no return.
            return_data = [[{"Error": "No Return Value"}], [{"Error": "No Return Value"}]]
            return


        # Now, to calculate the Balance and such. Will put into SQL as well.
        lp = "CFH"
        deposit = (float(account_info["Balance"]) if "Balance" in account_info else 0) + \
                  (float(account_info["ClosedPL"]) if "ClosedPL" in account_info else 0)

        pnl = float(account_info["OpenPL"]) if "OpenPL" in account_info else 0

        equity = (float(account_info["Balance"]) if "Balance" in account_info else 0) + \
                 (float(account_info["ClosedPL"]) if "ClosedPL" in account_info else 0) + \
                 (float(account_info["OpenPL"]) if "OpenPL" in account_info else 0) + \
                 (float(account_info["CreditLimit"]) if "CreditLimit" in account_info else 0)

        credit = float(account_info["SecurityDeposit"]) if "SecurityDeposit" in account_info else 0
        # credit =  account_info['CreditLimit']  if 'CreditLimit' in account_info else 0
        account_info['equity'] = equity

        total_margin = account_info['MarginRequirement'] if 'MarginRequirement' in account_info else 0
        free_margin = account_info['AvailableForMarginTrading'] if 'AvailableForMarginTrading' in account_info else 0

        database = "aaron"
        db_table = "lp_summary"
        # db_table = "lp_summary_copy"
        sql_insert = """INSERT INTO {database}.{db_table} (lp, deposit, pnl, equity, total_margin, free_margin, 
                credit, updated_time) VALUES ('{lp}', '{deposit}', '{pnl}', '{equity}', '{total_margin}', 
                '{free_margin}', '{credit}', now()) ON DUPLICATE KEY UPDATE deposit=VALUES(deposit), pnl=VALUES(pnl), 
                total_margin=VALUES(total_margin), equity=VALUES(equity), credit=VALUES(credit),
                free_margin=VALUES(free_margin), Updated_Time=VALUES(Updated_Time) """.format(database=database,
                                                                                              db_table=db_table, lp=lp,
                                                                                              deposit=deposit, pnl=pnl,
                                                                                              equity=equity,
                                                                                              total_margin="{:.2f}".format(
                                                                                                  float(total_margin)),
                                                                                              free_margin="{:.2f}".format(
                                                                                                  float(free_margin)),
                                                                                              credit=credit)

        self.db.engine.execute(sql_insert)  # Insert into DB

        if update_tool_time == 1:
            # Update the tool to run.
            sql_insert = "INSERT INTO  aaron.`monitor_tool_runtime` (`Monitor_Tool`, `Updated_Time`, `email_sent`) VALUES" + \
                         " ('{Tool}', now(), 0) ON DUPLICATE KEY UPDATE Updated_Time=now(), email_sent=VALUES(email_sent)".format(
                             Tool="CFH_FIX_Position")
            raw_insert_result = self.db.engine.execute(sql_insert)

        # ASYNC send to SQL.
        # async_sql_insert(app=current_app._get_current_object(), header="", values=[sql_insert], footer="")
        #
        # if update_tool_time == 1:
        #     async_update_Runtime(app=current_app._get_current_object(), Tool="CFH_FIX_Position")


        return

    async def heartbeat(self, connectionHandler, msg):
        sleep_from = datetime.datetime.now()
        print("Heartbeat: {} Time: {}".format(msg, sleep_from))



        #
        await self.getAccount_Position(connectionHandler)
        await self.getAccount_details(connectionHandler)
        print(self.client_account_info)
        print(self.client_open_position)

        await self.fix_position_sql_update()
        await self.chf_fix_details_ajax()

        # Need to clear the dict to prepare for new information
        self.client_account_info = dict()
        self.client_open_position = dict()


    # Want to get all open positions.
    async def getAccount_Position(self, connectionHandler):

        codec = connectionHandler.codec
        msg = FIXMessage("AN")
        msg.setField(codec.protocol.fixtags.PosReqID, "{}".format(datetime.datetime.now().timestamp()))
        #msg.setField(codec.protocol.fixtags.PosReqID, "ABC1234")
        msg.setField(codec.protocol.fixtags.PosReqType, 0)
        msg.setField(codec.protocol.fixtags.NoPartyIDs, 0)
        msg.setField(codec.protocol.fixtags.Account, self.client_num)
        msg.setField(codec.protocol.fixtags.AccountType, 1)
        msg.setField(codec.protocol.fixtags.ClearingBusinessDate, datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        msg.setField(codec.protocol.fixtags.TransactTime, datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        #msg.setField(codec.protocol.fixtags.PartyID, "197201")
        await connectionHandler.sendMsg(msg)

    async def account_logout(self, connectionHandler):

        codec = connectionHandler.codec
        msg = FIXMessage(codec.protocol.msgtype.LOGOUT)

        await connectionHandler.sendMsg(msg)

    # Want to get the account Details.
    async def getAccount_details(self, connectionHandler):
        codec = connectionHandler.codec

        #197201  - UAT
        # 27840 - Live
        msg = FIXMessage("AAA")
        msg.setField(codec.protocol.fixtags.Account, self.client_num)
        await connectionHandler.sendMsg(msg)


    async def onLogin(self, connectionHandler, msg):
        logging.info("Logged in")
        print("Login: {}".format(msg))
        await self.getAccount_Position(connectionHandler)
        await self.getAccount_details(connectionHandler)
        #


    async def onLogout(self, connectionHandler, msg):
        logging.info("Logged out")

        #self.loop.run_until_complete(self.loop.stop())
        #print("Before loop stop")
        pending = asyncio.Task.all_tasks(loop=self.loop)



        # for p in pending:  # Want to cancel all the pending task.
        #     try:
        #         p.cancel()
        #     except asyncio.CancelledError:
        #         print("{} : cancel_me is cancelled now".format(p))
        # #self.loop.run_until_complete(asyncio.gather(*pending))
        # asyncio.sleep(0.00001)
        self.loop.stop()
        # while self.loop.is_running():
        #     sleep(0.05)
        #     print("Loop Running... ")
        #self.loop.close()

# Get the position and account info from CFH FIX
def CFH_Position_n_Info():
    #logging.basicConfig(level=logging.DEBUG)
    #loop = asyncio.get_event_loop()    # only able to run in the main loop.
    loop = asyncio.new_event_loop()
    client = Client()
    #client.start(host='TS2LiveFix8.cfixtech.com', port=5308, loop=loop)
    #loop.run_until_complete(client.start(host='3018.DemoFixGW.com', port=5200, loop=loop))
    loop.run_until_complete(client.start(host='TS2LiveFix8.cfixtech.com', port=5308, loop=loop))
    loop.run_forever()
    # pending = asyncio.Task.all_tasks
    pending = asyncio.Task.all_tasks(loop=loop)
    for p in pending:   # Want to cancel all the pending task.
        try:
            p.cancel()
        except asyncio.CancelledError:
            print("{} : cancel_me is cancelled now".format(p))

    #print(pending)
    loop.close()
    #print(client.client_account_info)
    #print(client.client_open_position)
    return [client.client_account_info, client.client_open_position]




# CFH Fix works from UTC Sunday 1730 - Friday 2215
# GMT = UTC timing
# We want to allow for Sunday 1735 - Friday 2210 Connection.
# Giving 5 mins buffer.
def cfh_fix_timing():
    datetime_now = datetime.datetime.utcnow()
    weekd = datetime_now.weekday()  # Monday = 0

    # # For Testing
    # if weekd == 3 and datetime_now.time() > datetime.time(8, 7, 0):
    #     return False

    if weekd >= 4:
        if weekd == 4 and datetime_now.time() > datetime.time(22,10,0):  # Friday
            return False
        if weekd == 5:  # Sat
            return False
        if weekd == 6 and datetime_now.time() < datetime.time(17,35,0):   # Sunday
            return False
    return True


if __name__ == '__main__':
    while True:
        if cfh_fix_timing():
            CFH_Position_n_Info()
            print("CFH Position and info stopped. Sleeping for 5s")
            sleep(5)
        else:
            sleep(60)
