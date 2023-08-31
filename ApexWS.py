import asyncio
import copy
import threading

from apexpro.constants import APEX_WS_MAIN
from apexpro.websocket_api import WebSocket
from OrderbookData import OrderobookDataList
from ApexAPI import ApexAPI
from DisplaySystemMessage import DisplaySystemMessage


class ApexWSDataConverter:
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
        tmp_bids = {float(price): float(size) for price, size in bid_snap}
        tmp_asks = {float(price): float(size) for price, size in ask_snap}
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



class ApexWS:
    def __init__(self, target_base_currencies, num_recording_boards) -> None:
        self.ws = WebSocket(endpoint=APEX_WS_MAIN)
        self.target_base_currencies = target_base_currencies
        self.num_recording_boards = num_recording_boards
        self.data_converters = {} #symbol:class_instance
        self.symbols = []

    

    def callback_trade(self, message):
        '''
        if message['type'] == 'delta':
            ApexproTradeData.add_data(message)
            print(message['data'][-1]['s'] + '-' + message['data'][-1]['S'] + ': ' + str(message['data'][-1]['v'])+ ' @'+ str(message['data'][-1]['p']))
        elif message['type'] == 'snapshot':
            pass
        else:
            print('Unknown data type!')
            print(message)
        '''
        pass


    def __callback_depth(self, message):
        '''
        'topic': 'orderBook25.H.BTCUSDC', 'type': 'snapshot', 'data': {'s': 'BTCUSDC', 'b': [['26502.0', '11.600'], ['26502.5', '2.632'], ['26503.0', '5.760'], ['26503.5', '8.408'], ['26504.0', '8.472'], ['26504.5', '9.256'], ['26505.0', '4.624'], ['26505.5', '8.224'], ['26506.0', '4.680'], ['26506.5', '3.128'], ['26510.5', '1.888'], ['26511.5', '3.912'], ['26523.5', '0.227'], ['26530.0', '0.149'], ['26549.5', '13.920'], ['26550.0', '7.424'], ['26550.5', '7.704'], ['26551.0', '9.624'], ['26551.5', '2.064'], ['26552.0', '3.032'], ['26552.5', '4.168'], ['26553.0', '3.792'], ['26553.5', '1.712'], ['26554.0', '7.736'], ['26554.5', '6.856']], 'a': [['26555.0', '1.755'], ['26555.5', '0.225'], ['26556.0', '0.888'], ['26559.5', '0.001'], ['26560.0', '4.528'], ['26561.0', '6.297'], ['26561.5', '3.792'], ['26562.0', '5.720'], ['26562.5', '5.721'], ['26563.5', '7.736'], ['26565.5', '4.865'], ['26567.5', '2.796'], ['26568.0', '3.568'], ['26606.0', '3.304'], ['26608.0', '1.560'], ['26608.5', '4.202'], ['26609.0', '2.624'], ['26609.5', '3.552'], ['26610.0', '11.128'], ['26610.5', '11.592'], ['26611.0', '9.632'], ['26611.5', '11.744'], ['26612.0', '5.008'], ['26612.5', '1.088'], ['26626.5', '5.439']], 'u': 5496963}, 'cs': 1149719309, 'ts': 1692339921178495}
        {'topic': 'orderBook25.H.BTCUSDC', 'type': 'delta', 'data': {'s': 'BTCUSDC', 'b': [['26554.0', '0'], ['26501.5', '11.456']], 'a': [['26555.5', '1.785']], 'u': 5496964}, 'cs': 1149719309, 'ts': 1692339921178499}
        '''
        symbol = message['data']['s']
        if symbol not in self.symbols:
            self.symbols.append(symbol)
            self.data_converters[symbol] = ApexWSDataConverter(symbol, self.target_base_currencies, self.num_recording_boards)
        if message['type'] == 'snapshot':
            flg = self.data_converters[symbol].add_snapshot(message['data']['b'].copy(), message['data']['a'].copy())
            if flg:
                OrderobookDataList.add_data('apex', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), message['ts'])
        elif message['type'] == 'delta':
            flg = self.data_converters[symbol].add_delta(message['data']['b'].copy(), message['data']['a'].copy())
            if flg:
                OrderobookDataList.add_data('apex', symbol, self.data_converters[symbol].bids.copy(), self.data_converters[symbol].asks.copy(), message['ts'])
        else:
            DisplaySystemMessage.display_error('ApexWS', 'Invalid ws type in Apex!' + '\r\n'+ 'type='+message['type'])
            


    



    async def get_all_tickers(self):
        api = ApexAPI()
        tickers = await api.get_tickers()
        return tickers


    async def start(self):
        tickers = await self.get_all_tickers()
        for symbol, base_currency in zip(tickers['symbols'], tickers['base_currency']):
            if base_currency in self.target_base_currencies:
                #self.ws.trade_stream(self.callback_trade, symbol)
                self.ws.depth_stream(self.__callback_depth, symbol,25)
                OrderobookDataList.setup_new_ex_symbol('apex', symbol)
        while True:
            await asyncio.sleep(0.1)



if __name__ == '__main__':
    OrderobookDataList.initialize()
    ws = ApexWS(['BTC', 'ETH'], 3)
    asyncio.run(ws.start())
