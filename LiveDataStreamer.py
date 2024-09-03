from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from datetime import datetime
import threading
import csv
from ibapi.ticktype import TickTypeEnum
import time
import os
from csv_operations import save_historical_data, save_market_data

port = 7497

class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.orderId = None
        self.historical_data = []
        self.last_minute = None
        self.tick_buffer = []  # Buffer to hold ticks before writing to the CSV file
        self.buffer_limit = 1  # Number of ticks to buffer before writing to the file
        self.historical_data_path = 'historical_data.csv'
        self.tick_data_path = 'market_data.csv'

    def nextValidId(self, orderId):
        self.orderId = orderId
    
    def nextId(self):
        self.orderId += 1
        return self.orderId
    
    def error(self, reqId, errorCode, errorString):
        """Handle errors by printing details unless the error code is 2176."""
        if errorCode != 2176:
            print(f"reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}")


    def tickPrice(self, reqId: int, tickType, price, attrib):
        """Handles tick price updates."""
        save_market_data(self.tick_data_path, self.tick_buffer, tickType, price, self.buffer_limit)

    def historicalData(self, reqId, bar):
        # Scale volume by a factor of 100
        scaled_volume = bar.volume * 100
        
        # Collect the historical data with scaled volume
        self.historical_data.append([
            bar.date, bar.open, bar.high, bar.low, bar.close, scaled_volume
        ])
    
    def historicalDataEnd(self, reqId, start, end):

        # Save the historical data using the csv_operations module
        num_new_rows = save_historical_data(self.historical_data_path, self.historical_data)
        
        # Clear the historical data after saving
        self.historical_data = []
        

    def monitor_time_and_request_historical_data(self, contract):
        while True:
            current_time = datetime.now()
            current_second = current_time.second

            # Calculate the seconds until the next 2-second mark
            seconds_until_next_request = (2 - current_second % 60) % 60

            if seconds_until_next_request <= 0:
                # Prepare the request parameters
                req_id = self.nextId()
                formatted_time = current_time.strftime('%Y%m%d %H:%M:%S US/Eastern')
                
                # Debugging output
                #print(f"Request ID: {req_id}")
                #print(f"Request Time: {formatted_time}")
                #print(f"Contract: {contract}")

                # Perform the historical data request
                try:
                    self.reqHistoricalData(req_id, contract, formatted_time, "1 D", "1 min", "TRADES", 0, 1, False, [])
                except Exception as e:
                    print(f"Error making request: {e}")

                # Sleep until the next second to prevent multiple requests within the same second
                time.sleep(1)

            # Sleep for a short duration before checking the time again
            time.sleep(0.5)  # Check every 0.5 seconds


app = TestApp()
app.connect("127.0.0.1", port, 0)
threading.Thread(target=app.run).start()
time.sleep(1)

mycontract = Contract()
mycontract.symbol = "NVDA"
mycontract.secType = "STK"
mycontract.exchange = "SMART"
mycontract.currency = "USD"

app.reqMarketDataType(1)
app.reqMktData(app.nextId(), mycontract, "", False, False, [])
# Start a separate thread to monitor the time and request historical data
threading.Thread(target=app.monitor_time_and_request_historical_data, args=(mycontract,)).start()
