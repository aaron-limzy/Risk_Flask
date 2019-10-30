import asyncio
import logging
import socket

from aiopyfix.FIX44 import fixtags
from aiopyfix.journaler import DuplicateSeqNoError
from aiopyfix.message import FIXMessage
from aiopyfix.session import FIXSession
from aiopyfix.connection import FIXEndPoint, ConnectionState, MessageDirection, FIXConnectionHandler


class FIXClientConnectionHandler(FIXConnectionHandler):
    def __init__(self, engine, protocol, targetCompId, senderCompId, reader, writer, addr=None, observer=None,
                 targetSubId=None, senderSubId=None, heartbeatTimeout=30, heartbeat=1,  userName=None, password=None):
        FIXConnectionHandler.__init__(self, engine, protocol, reader, writer, addr, observer)

        self.targetCompId = targetCompId
        self.senderCompId = senderCompId
        self.targetSubId = targetSubId
        self.senderSubId = senderSubId
        self.heartbeatPeriod = float(heartbeatTimeout)
        self.heartbeat = heartbeat
        self.userName = userName
        self.password = password

        # we need to send a login request.
        self.session = self.engine.getOrCreateSessionFromCompIds(self.targetCompId, self.senderCompId)
        if self.session is None:
            raise RuntimeError("Failed to create client session")

        self.protocol = protocol

        asyncio.ensure_future(self.logon())

    async def logon(self):

        self.session.resetMsgs()    # Reset the number.
        logging.info("FIXClientConnectionHandler, in logon.")
        logonMsg = self.protocol.messages.Messages.logon()
        if self.userName:   # Want to add username, if provided.
            logonMsg.setField(self.protocol.fixtags.Username, self.userName)
        if self.password: # Want to add password, if provided.
            logonMsg.setField(self.protocol.fixtags.Password, self.password)

        logonMsg.setField(self.protocol.fixtags.ResetSeqNumFlag, "Y")
        await self.sendMsg(logonMsg)


    async def handleSessionMessage(self, msg):
        protocol = self.codec.protocol
        responses = []


        logging.info("handleSessionMessage Recieved: {}".format(msg))
        recvSeqNo = msg[protocol.fixtags.MsgSeqNum]

        msgType = msg[protocol.fixtags.MsgType]
        targetCompId = msg[protocol.fixtags.TargetCompID]
        senderCompId = msg[protocol.fixtags.SenderCompID]

        if msgType == protocol.msgtype.LOGON:
            if self.connectionState == ConnectionState.LOGGED_IN:
                logging.warning("Client session already logged in - ignoring login request")
            else:
                try:
                    self.connectionState = ConnectionState.LOGGED_IN
                    self.heartbeatPeriod = float(msg[protocol.fixtags.HeartBtInt])
                except DuplicateSeqNoError:
                    logging.error("Failed to process login request with duplicate seq no")
                    await self.disconnect()
                    return
        elif self.connectionState == ConnectionState.LOGGED_IN:
            # compids are reversed here
            if not self.session.validateCompIds(senderCompId, targetCompId):
                logging.error("Received message with unexpected comp ids")
                await self.disconnect()
                return

            if msgType == protocol.msgtype.LOGOUT:
                self.connectionState = ConnectionState.LOGGED_OUT
                self.handle_close()
            elif msgType == protocol.msgtype.TESTREQUEST:
                responses.append(protocol.messages.Messages.heartbeat())
            elif msgType == protocol.msgtype.RESENDREQUEST:
                responses.extend(self._handleResendRequest(msg))
            elif msgType == protocol.msgtype.SEQUENCERESET:
                # we can treat GapFill and SequenceReset in the same way
                # in both cases we will just reset the seq number to the
                # NewSeqNo received in the message
                newSeqNo = msg[protocol.fixtags.NewSeqNo]
                if msg[protocol.fixtags.GapFillFlag] == "Y":
                    logging.info("Received SequenceReset(GapFill) filling gap from %s to %s" % (recvSeqNo, newSeqNo))
                self.session.setRecvSeqNo(int(newSeqNo) - 1)
                recvSeqNo = newSeqNo
        else:
            logging.warning("Can't process message, counterparty is not logged in...")

        return (recvSeqNo, responses)


class FIXClient(FIXEndPoint):
    def __init__(self, engine, protocol, targetCompId, senderCompId, targetSubId=None, senderSubId=None,
                 heartbeatTimeout=30, withSeqNoReset=True, userName=None, password=None, heartbeat=30):
        self.targetCompId = targetCompId
        self.senderCompId = senderCompId
        self.targetSubId = targetSubId
        self.senderSubId = senderSubId
        self.heartbeatTimeout = heartbeatTimeout
        self.heartbeat = heartbeat
        self.withSeqNoReset = withSeqNoReset
        self.userName = userName
        self.password = password
        self.reader = self.writer = None
        self.addr = None

        FIXEndPoint.__init__(self, engine, protocol)

    async def start(self, host, port, loop):
        self.reader, self.writer = await asyncio.open_connection(host, port, loop=loop)
        self.addr = (host, port)
        logging.info("Connected to %s" % repr(self.addr))

       #observer = None,
        connection = FIXClientConnectionHandler(engine=self.engine, protocol=self.protocol, targetCompId=self.targetCompId,
                                                senderCompId=self.senderCompId, reader=self.reader, writer=self.writer,
                                                addr=self.addr, targetSubId=self.targetSubId,
                                                senderSubId=self.senderSubId, observer=self,
                                                heartbeat=30,
                                                heartbeatTimeout=self.heartbeatTimeout,
                                                userName=self.userName, password=self.password)
        self.connections.append(connection)
        for handler in filter(lambda x: x[1] == ConnectionState.CONNECTED, self.connectionHandlers):
            await handler[0](connection)

    def stop(self):
        logging.info("Stopping client connections")
        for connection in self.connections:
            connection.disconnect()
        self.writer.close()
