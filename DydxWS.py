import asyncio
import json
import websockets
import threading
import time
from web3 import Web3
from dydx3 import Client
from dydx3.helpers.request_helpers import generate_now_iso
from dydx3.constants import API_HOST_GOERLI
from dydx3.constants import NETWORK_ID_GOERLI
from dydx3.constants import WS_HOST_GOERLI
from DydxAPI import DydxAPI
from OrderbookData import OrderobookDataList


'''
websocketsは3.10以上だと以下のエラーが出る。
As of 3.10, the *loop* parameter was removed from Lock() since it is no longer necessary
'''

class DydxWSDataConverter:
    def __init__(self, symbol, target_base_currencies, num_recording_boards) -> None:
        self.target_base_currencies = target_base_currencies
        self.num_recording_boards = num_recording_boards
        self.symbol = symbol
        self._bids = {}
        self._asks = {}
        self._lock = threading.RLock()

    @property
    def bids(self):
        with self._lock:
            return self._bids

    @bids.setter
    def bids(self, new_value):
        with self._lock:
            self._bids = new_value

    @property
    def asks(self):
        with self._lock:
            return self._asks

    @asks.setter
    def asks(self, new_value):
        with self._lock:
            self._asks = new_value

    def add_snapshot(self, bid_snap, ask_snap):
        tmp_bids = {float(item["price"]): float(item["size"]) for item in bid_snap}
        tmp_asks = {float(item["price"]): float(item["size"]) for item in ask_snap}
        #tmp_bids = {float(price): float(size) for price, size in bid_snap}
        #tmp_asks = {float(price): float(size) for price, size in ask_snap}
        tmp_bids = sorted(tmp_bids.items(), key=lambda x: x[0], reverse=True)  # bidsを価格が高い順にソート
        tmp_asks = sorted(tmp_asks.items())  # asksは価格が低い順にソート
        #bids, asksをそれぞれ3番目までのデータのみを残す。
        tmp_bids = dict(tmp_bids[:self.num_recording_boards])
        tmp_asks = dict(tmp_asks[:self.num_recording_boards])
        flg = False
        bids = self.bids
        if tmp_bids != bids:
            self.bids = tmp_bids.copy()
            flg = True
        asks = self.asks
        if tmp_asks != asks:
            self.asks = tmp_asks.copy()
            flg = True
        return flg


    def add_delta(self, delta_bids, delta_asks):
        flg = False
        if len(delta_bids) > 0:
            tmp_bids = self.bids.copy()
            delta_bids = {float(price):float(size) for price, size in delta_bids}
            tmp_bids.update(delta_bids)
            tmp_bids = {price:size for price, size in tmp_bids.items() if size >0}
            tmp_bids = sorted(tmp_bids.items(), key=lambda x: x[0], reverse=True)
            tmp_bids = dict(tmp_bids[:self.num_recording_boards])
            if tmp_bids != self.bids:
                self.bids = tmp_bids.copy()
                flg = True
        if len(delta_asks) > 0:
            tmp_asks = self.asks.copy()
            delta_asks = {float(price):float(size) for price, size in delta_asks}
            tmp_asks.update(delta_asks)
            tmp_asks = {price:size for price, size in tmp_asks.items() if size >0}
            tmp_asks = sorted(tmp_asks.items())  # asksは価格が低い順にソート
            tmp_asks = dict(tmp_asks[:self.num_recording_boards])
            if tmp_asks != self.bids:
                self.asks = tmp_asks.copy()
                flg = True
        return flg



class DydxWS:
    def __init__(self, target_base_currencies, num_recording_boards) -> None:
        self.target_base_currencies = target_base_currencies
        self.num_recording_boards = num_recording_boards
        self.data_converters = {} #symbol:DydxWSDataConverter


    def __callback(self, message):
        if message['type'] != 'connected':
            symbol = message['id']
            if message['type'] == 'subscribed' and len(message['contents']) > 0: #snapshot
                #print(message['contents']['bids'][0], message['contents']['asks'][0])
                flg = self.data_converters[symbol].add_snapshot(message['contents']['bids'], message['contents']['asks'])
                if flg:
                    #print(list(self.data_converters[message['id']].asks.items())[0])
                    #print(message['id'],' : ', list(self.data_converters[message['id']].asks.items())[0], list(self.data_converters[message['id']].bids.items())[0])
                    OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))
            elif message['type'] == 'channel_data':
                bid = {}
                ask = {}
                if len(message['contents']['bids']) > 0:
                    bid = message['contents']['bids']
                if len(message['contents']['asks']) > 0:
                    ask = message['contents']['asks']
                #print(bid, ask)
                flg = self.data_converters[message['id']].add_delta(bid, ask)
                if flg:
                    #print(message['id'],' : ', list(self.data_converters[message['id']].asks.items())[0], list(self.data_converters[message['id']].bids.items())[0])
                    OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))



    async def start(self):
        '''
        snapshot {"type":"subscribed","connection_id":"f40dbd7b-2ce4-4955-a475-18f2b9c58872","message_id":1,"channel":"v3_orderbook","id":"BTC-USD","contents":{"asks":[{"size":"5.4686","price":"25867"},{"size":"1.977","price":"25868"},{"size":"3.4752","price":"25869"},{"size":"6.0747","price":"25870"},{"size":"3.7257","price":"25871"},{"size":"7.3402","price":"25872"},{"size":"2.8043","price":"25873"},{"size":"10.547","price":"25874"},{"size":"1.6915","price":"25875"},{"size":"0.874","price":"25876"},{"size":"1.489","price":"25877"},{"size":"2.7652","price":"25878"},{"size":"0.7149","price":"25880"},{"size":"5.9811","price":"25881"},
        delta "type":"channel_data","connection_id":"f40dbd7b-2ce4-4955-a475-18f2b9c58872","message_id":2,"id":"BTC-USD","channel":"v3_orderbook","contents":{"offset":"27672358048","bids":[["25861","9.2948"]],"asks":[]}}
        '''
        async with websockets.connect('wss://api.dydx.exchange/v3/ws') as websocket:
            api = DydxAPI()
            tickers = await api.get_tickers()
            for symbol, base_currency in zip(tickers['symbols'], tickers['base_currency']):
                if base_currency in self.target_base_currencies:
                    req = {
                        'type': 'subscribe',
                        'channel': 'v3_orderbook',
                        'id': symbol
                    }
                    self.data_converters[symbol] = DydxWSDataConverter(symbol, self.target_base_currencies, self.num_recording_boards)
                    OrderobookDataList.setup_new_ex_symbol('dydx', symbol)
                    await websocket.send(json.dumps(req))
            while True:
                res = await websocket.recv()
                res = json.loads(res)
                self.__callback(res)
                #print(f'< {res}')



if __name__ == '__main__':
    api = DydxAPI()
    tickers = api.get_tickers()
    ws = DydxWS([],5)
    #asyncio.get_event_loop().run_until_complete(ws.start())
    asyncio.run(ws.start_orderbookdata(list(tickers['symbols'])[:20]))