import pytest
from flask import Flask
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
app = Flask(__name__)

window = 10
interval = 1

#Extract status and circuitID, and queue the message for analysis.
def analyzeNotification(message):
    xml = ET.fromstring(message)
    circuit = xml.find('.//Circuit').text
    if xml.find('.//sendGroundFaultEvent'):
        status = 'fault'
    else:
        status = 'restoration'
    return (circuit, (status, message))

#Update status of each circuitID every interval.
def timerElapsed(row, state):
    if row:
        status = row[0][0]
        message = row[0][1]
        time = datetime.now()
        if not state:
            if status == 'fault':
                state = (time, status, message, False)
        elif state[1] == 'fault':
            if status == 'restoration':
                state = (state[0], status, message, False)
    if state:
        lastFailedTime = state[0]
        delta = datetime.now() - lastFailedTime
        if delta >= timedelta(seconds = window):
            if state[3] or state[1] == 'fault':
                state = None
            elif state[1] == 'restoration':
                state = (lastFailedTime, state[1], state[2], True)
    return state

    #Extracts the XML from the RDDs that need to be forwarded.
def forward(row):
    toForward = row[1][3]
    return toForward

def getXml(row):
    xml = row[1][2]
    return xml

@app.route('/analyzeNotification')
def test_analyzeNotification():
    message = '<soapenv:Envelope ' \
              'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:dp="http://www.datapower.com/schemas/management" ' \
              'xmlns:dpconfig="http://www.datapower.com/param/config" ' \
              'xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:json="http://www.ibm.com/xmlns/prod/2009/jsonx" ' \
              'xmlns:typ="http://www.ibm.com/wbe/casoap/types" ' \
              'xmlns:xs="http://www.w3.org/2001/XMLSchema" ' \
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soapenv:Header />' \
              '<soapenv:Body><typ:ODMRestorationStatus><RestorationStatus>' \
              '<CorrelationID>f31b8074-6f61-463d-87bf-cf4eca074110</CorrelationID>' \
              '<Timestamp>2021-09-29T03:01:08Z</Timestamp><JobID>NW21092900005</JobID>' \
              '<TroubleCode>PPWD</TroubleCode><Circuit>MALTA9700</Circuit>' \
              '<RestorationStatusPayload>pppppppppp__9700__pppppppppp</RestorationStatusPayload>' \
              '</RestorationStatus></typ:ODMRestorationStatus></soapenv:Body>' \
              '</soapenv:Envelope>'
    if analyzeNotification(message) == 'restoration':
        return "Test passed for GroundFault event"
    else:
        return "Test passed for Restoration event"

@app.route('/timerElapsed')
def test_timerElapsed():
    row = [('fault', 'message')]
    state = None
    if timerElapsed(row, state) == (datetime.now(), 'fault', 'message', False):
        return 'Test Passed'
    else:
        return 'Test Failed'

@app.route('/forward')
def test_forward():
    row = ('MALTA7600', (datetime.now(), 'fault', 'message', True))
    if forward(row) == True:
        return 'Test Passed'
    else:
        return 'Test Failed'

@app.route('/getXml')
def test_getXml():
    row = ('MALTA7600', (datetime.now(), 'fault', 'message', True))
    if getXml(row) == 'message':
        return 'Test Passed'
    else:
        return 'Test Failed'

if __name__ == "__main__":
    app.run(debug=True, port=8000)

#test_analyzeNotification()
#test_timerElapsed()
#test_forward()
#test_getXml()

#stream = ssc.socketTextStream('localhost', 9999)
#stream = stream.map(analyzeNotification)
#stream = stream.updateStateByKey(timerElapsed)
#stream = stream.filter(forward)
#stream = stream.map(getXml)
#stream.pprint()

#ssc.start()
#ssc.awaitTermination()


