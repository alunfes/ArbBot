import asyncio
import copy

from apexpro.constants import APEX_WS_MAIN
from apexpro.websocket_api import WebSocket
from OrderbookData import OrderobookDataList
from ApexAPI import ApexAPI
from DisplaySystemMessage import DisplaySystemMessage



class ApexWS:
    def __init__(self, target_base_currencies, num_recording_boards) -> None:
        self.target_base_currencies = target_base_currencies
        self.num_recording_boards = num_recording_boards
        self.key = ''
        self.ws = WebSocket(endpoint=APEX_WS_MAIN)
        self.symbols = []
        self.bids = {}
        self.asks = {}
    

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
        if message['type'] == 'snapshot':
            flg = self.__add_snapshot(message['data']['b'], message['data']['a'])
            if flg:
                OrderobookDataList.add_data('apex', symbol, self.bids, self.asks, message['ts'])
        elif message['type'] == 'delta':
            self.__add_delta(message['data']['b'], message['data']['a'])
            OrderobookDataList.add_data('apex', symbol, self.bids, self.asks, message['ts'])
        else:
            DisplaySystemMessage.display_error('ApexWS', 'Invalid ws type in Apex!' + '\r\n'+ 'type='+message['type'])
            
        
    
    def __add_snapshot(self, bid_snap, ask_snap):
        bids = {float(price): float(size) for price, size in bid_snap}
        asks = {float(price): float(size) for price, size in ask_snap}
        bids = sorted(bids.items(), key=lambda x: x[0], reverse=True)  # bidsを価格が高い順にソート
        asks = sorted(asks.items())  # asksは価格が低い順にソート
        #bids, asksをそれぞれ3番目までのデータのみを残す。
        bids = dict(bids[:self.num_recording_boards])
        asks = dict(asks[:self.num_recording_boards])
        flg = False
        if bids != self.bids:
            self.bids = self.bids.copy.deepcopy()
            flg = True
        if asks != self.asks:
            self.asks = self.asks.copy.deepcopy()
            flg = True
        return flg
        


    def __add_delta(self, delta_bids, delta_asks):
        for price_str, size_str in delta_bids:
            price = float(price_str)
            size = float(size_str)
            # sizeが0なら、その価格の注文を削除
            if size == 0:
                self.bids.pop(price, None)
            # そうでない場合は、bidsを更新 (新しい価格の場合は追加)
            else:
                self.bids[price] = size
        for price_str, size_str in delta_asks:
            price = float(price_str)
            size = float(size_str)
            # sizeが0なら、その価格の注文を削除
            if size == 0:
                self.asks.pop(price, None)
            # そうでない場合は、bidsを更新 (新しい価格の場合は追加)
            else:
                self.asks[price] = size
        self.bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)  # bidsを価格が高い順にソート
        self.asks = sorted(self.asks.items())  # asksは価格が低い順にソート
        self.bids = dict(self.bids[:self.num_recording_boards])
        self.asks = dict(self.asks[:self.num_recording_boards])
        


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
