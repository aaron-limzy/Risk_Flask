import asyncio
from enum import Enum
import logging
import random
from aiopyfix.connection import ConnectionState, MessageDirection
from aiopyfix.client_connection import FIXClient
from aiopyfix.engine import FIXEngine
from aiopyfix.message import FIXMessage
from datetime import datetime
from time import sleep

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

    async def position_info(self, connectionHandler, msg):   # If it replies with the account Info.
        logging.info("account position recieved: {}".format(msg))
        codec = connectionHandler.codec
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
        await self.account_logout(connectionHandler)
        #connectionHandler.handle_close()
        #self.loop.stop()  # kill the Loop.

        # for m in msg.tags:
        #     print(m)

    async def heartbeat(self, connectionHandler, msg):
        pass

    # Want to get all open positions.
    async def getAccount_Position(self, connectionHandler):

        codec = connectionHandler.codec
        msg = FIXMessage("AN")
        msg.setField(codec.protocol.fixtags.PosReqID, "ABC1234")
        msg.setField(codec.protocol.fixtags.PosReqType, 0)
        msg.setField(codec.protocol.fixtags.NoPartyIDs, 0)
        msg.setField(codec.protocol.fixtags.Account, self.client_num)
        msg.setField(codec.protocol.fixtags.AccountType, 1)
        msg.setField(codec.protocol.fixtags.ClearingBusinessDate, datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        msg.setField(codec.protocol.fixtags.TransactTime, datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
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


