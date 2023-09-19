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

'offset': '27930794929'の値が小さいものから順番に板に反映させる必要があるが、wsの配信は必ずしもその順番通りになっているとは限らない。
3つくらいデータを保持しておいて、データが追加されたら最もoffset値が小さいものを板に反映させる。
反映が遅くなるし、3つでも絶対にそれ以上の配信遅れが発生しないとは言い切れない。

別の方法としては、受信したデータは即時板に反映させてデータを記録しておく。
もし既に反映させたデータよりも前のoffsetのデータを受信した場合は反映済みのデータを元に戻してより古いものから反映させ直す。
受信-データ記録-処理
'''
class DydxWSData:
    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self._lock = threading.RLock()
        self._bid_ask_data = {} #offset:contents
        self._offset_list = []
        self._latest_offset = 0
    
    def add_data(self, delta_data):
        self._offset_list.append(delta_data['contents']['offset'])
        self._bid_ask_data[delta_data['contents']['offset']] = delta_data['contents']



    def set_data(self, delta_data):
        '''
        データを取得するたびにnext offset値よりも小さいかを確認してflgを立てる。
        '''
        with self._lock:
            offset = int(delta_data['contents']['offset'])
            self._bid_ask_data[offset] = delta_data['contents']
            if self._next_offset >= offset:
                self._next_offset = offset + 1
                self._latest_offset = offset
                self._flg_next_offset_data_avaialble = True
            else:
                self._flg_next_offset_data_avaialble = False
                
    
    def get_data(self):
        with self._lock:
            return self.bid_ask_data.copy()
    

    def get_next_offset_data(self):
        '''

        '''
        with self._lock:
            if self._flg_next_offset_data_avaialble:
                self._flg_next_offset_data_avaialble = False
                return self._bid_ask_data[self._latest_offset]
            else:
                return None




class DydxWSDataConverter:
    '''
    contentsデータを追加する。
    offsetリストを追加する。
    最新のoffsetよりも大きな値かを確認する。
    true: add_delta
    false: detect all target offsets and add_delta for all of them
    '''
    def __init__(self, symbol, target_base_currencies, num_recording_boards) -> None:
        self.target_base_currencies = target_base_currencies
        self.num_recording_boards = num_recording_boards
        self.symbol = symbol
        self._bids = {} #price:size
        self._asks = {} #price:size
        self._lock = threading.RLock()
        self._func_lock = threading.RLock()

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
        with self._func_lock:
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
        with self._func_lock:
            flg = False
            if len(delta_bids) > 0:
                tmp_bids = self.bids.copy()
                converted_delta_bids = {float(price):float(size) for price, size in delta_bids}
                #if 0 in list(converted_delta_bids.values()):
                #    print(converted_delta_bids)
                tmp_bids.update(converted_delta_bids)
                tmp_bids = {price:size for price, size in tmp_bids.items() if size >0}
                tmp_bids = sorted(tmp_bids.items(), key=lambda x: x[0], reverse=True)
                tmp_bids = dict(tmp_bids[:self.num_recording_boards])
                if tmp_bids != self.bids:
                    self.bids = tmp_bids.copy()
                    flg = True
                    #print('bid-delta:', self.bids)
            if len(delta_asks) > 0:
                tmp_asks = self.asks.copy()
                converted_delta_asks = {float(price):float(size) for price, size in delta_asks}
                #if 0 in list(converted_delta_asks.values()):
                #    print(converted_delta_asks)
                tmp_asks.update(converted_delta_asks)
                tmp_asks = {price:size for price, size in tmp_asks.items() if size >0}
                tmp_asks = sorted(tmp_asks.items())  # asksは価格が低い順にソート
                tmp_asks = dict(tmp_asks[:self.num_recording_boards])
                if tmp_asks != self.asks:
                    self.asks = tmp_asks.copy()
                    flg = True
                    #print('ask-delta:', self.asks)
            return flg


class DydxWS:
    def __init__(self, target_base_currencies, num_recording_boards) -> None:
        self.target_base_currencies = target_base_currencies
        self.num_recording_boards = num_recording_boards
        self.data_converters = {} #symbol:DydxWSDataConverter
        self.ws_data = {} #symbol:DydxWSData
        self._contents = {} #offset:contents
        self._offset_list = []
        self._last_offset = 0
        self._num_log_delta = 50


    async def __callback(self, message):
        if message['type'] != 'connected':
            symbol = message['id']
            if message['type'] == 'subscribed' and len(message['contents']) > 0: #snapshot
                flg = self.data_converters[symbol].add_snapshot(message['contents']['bids'], message['contents']['asks'])
                if flg:
                    OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))
            elif message['type'] == 'channel_data':
                self.ws_data[symbol].set_data(message)
                next_offset_data = self.ws_data[symbol].get_next_offset_data()
                if next_offset_data != None:
                    flg = self.data_converters[symbol].add_delta(next_offset_data['bids'], next_offset_data['asks'])
                    if flg:
                        OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))


    async def __callback2(self, message):
        '''
        受信したdeltaデータのoffsetが今まで処理したものよりも大きいことを確認したら板に反映させる。
        小さい場合は、rollbackとして受信したもの以降記録しているすべてのデータを順に反映させて、データを消す。
        ・消した後に消したoffsetよりも小さなデータを受信した場合には板が正しく更新できない可能性がある。
        ->実際にこの現象が起こるためにrollbackで反映させたoffsetもある程度記録しておく必要がある。
        案1:offsetをリストに記録してsortしてそのうちx個以上のデータはcotents dictから削除する。
        案2:offsetをリストに記録して
        
        '''
        if message['type'] != 'connected':
            symbol = message['id']
            if message['type'] == 'subscribed' and len(message['contents']) > 0: #snapshot
                flg = self.data_converters[symbol].add_snapshot(message['contents']['bids'], message['contents']['asks'])
                if flg:
                    OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))
            elif message['type'] == 'channel_data':
                #record data
                offset = int(message['contents']['offset'])
                self._contents[offset] = message['contents']                
                self._offset_list.append(offset)
                #check if rollback is needed
                if offset > self._last_offset:
                    flg = self.data_converters[symbol].add_delta(self._contents[offset]['bids'], self._contents[offset]['asks'])
                    if flg:
                        OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))
                else:#roll back
                    self._offset_list.sort()
                    start_ind = self._offset_list.index(offset)
                    for target_offset in self._offset_list[start_ind:]:
                        contents_data = self._contents[target_offset]
                        flg = self.data_converters[symbol].add_delta(contents_data['bids'], contents_data['asks'])
                        if flg:
                            OrderobookDataList.add_data('dydx', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), int(time.time()))                    
                    #remove old contents data
                    if len(self._contents) > self._num_log_delta:
                        #self._contents = {key: value for key, value in self._contents.items() if key in self._offset_list}
                        for key in self._offset_list[:-self._num_log_delta]:
                            self._contents.pop(key, None)
                        self._offset_list = self._offset_list[-self._num_log_delta:]
                    #print('Rollback done for ', offset)
                self._last_offset = offset

                


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
                        'id': symbol,
                        #'includeOffsets': True
                    }
                    self.data_converters[symbol] = DydxWSDataConverter(symbol, self.target_base_currencies, self.num_recording_boards)
                    self.ws_data[symbol] = DydxWSData(symbol)
                    OrderobookDataList.setup_new_ex_symbol('dydx', symbol)
                    await websocket.send(json.dumps(req))
            while True:
                res = await websocket.recv()
                res = json.loads(res)
                await self.__callback2(res)




if __name__ == '__main__':
    OrderobookDataList.initialize(False)
    api = DydxAPI()
    tickers = asyncio.run(api.get_tickers())
    ws = DydxWS(tickers['base_currency'],5)
    #ws = DydxWS(['SOL'],5)
    #asyncio.get_event_loop().run_until_complete(ws.start())
    asyncio.run(ws.start())