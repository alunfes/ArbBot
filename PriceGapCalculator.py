import threading
import asyncio
import time
import pandas as pd

from OrderbookData import OrderobookDataList
from Params import Params
from PriceGapData import PriceGapData


'''
任意のsymbolと同じものを全ての取引所から見つける。
USDC/USDTペアの値を取得する。
USDTに調整する。
最大のbid/ask差があるペアを見つけてその差分を計算する。(各ペアでmarket entryしたとした時のbid/askの差)

取引所ごとのsymbolをリストとして取得する。
任意の取引所を基準として、その取引所の対象symbolを0番目から対象としてループで回す。
チェックしたsymbolはリストから外す。
'''

class PriceGapCalculator:
    def __init__(self, flg_write_gap_data) -> None:
        self.flg_write_gap_data = flg_write_gap_data
    
    async def start(self):
        self.flg_created_file = False
        PriceGapData.initialize()
        await asyncio.sleep(60) #wait till bid/ask data of all target symbols become available\
        self.ex_names = OrderobookDataList.get_ex_names()
        self.symbols = {} #ex:symbols
        for ex in self.ex_names:
            self.symbols[ex] = OrderobookDataList.get_symbols(ex)
        while True:
            time_sta = time.time()
            #毎回listから削除しながら同一銘柄の価格差を検証するためのリストをコピーして作成する。
            ex_names = self.ex_names.copy()
            symbols = {}
            for ex in self.ex_names:
                symbols[ex] = OrderobookDataList.get_symbols(ex)
            usdc_usdt = await self.__get_usdt_pair_price()
            kijun_ex = 'bybit'
            result_dict_list = [] #{symbol, max_ex, min_ex, gap_ratio}
            for symbol in symbols[kijun_ex]:
                #get prices of all same symbols
                prices = await self.__get_same_symbol_prices(symbol.replace(Params.quotes['bybit'],''), ex_names, symbols)
                #calc max price gap
                max_gap_ratio, long_ex, short_ex, max_bid, min_ask = await self.__calc_price_gap(prices, usdc_usdt)
                result_dict_list.append({'symbol':symbol, 'max_gap_ratio':max_gap_ratio, 'long_ex':long_ex, 'long price':min_ask, 'short_ex':short_ex, 'min_bid':max_bid})
                PriceGapData.add_gap_price(symbol, max_gap_ratio, long_ex, short_ex, max_bid, min_ask)
            max_symbol, max_gap, largest_long_price, largest_short_price, largest_long_ex, largest_short_ex = PriceGapData.get_max_gap_data()
            print('time=', round(time.time()-time_sta,4), 'max_gap_symbol:',max_symbol, ', gap_raio=',round(max_gap,6),  ', long price=',largest_long_price, ', long ex=',largest_long_ex, ', short price=',largest_short_price, ', short ex=',largest_short_ex)
            if self.flg_write_gap_data:
                await self.__write_max_gap_data(max_symbol, max_gap, largest_long_price, largest_short_price, largest_long_ex, largest_short_ex)
            if largest_long_ex == largest_short_ex and max_gap > 0:
                for ex in self.ex_names:
                    symbols[ex] = OrderobookDataList.get_symbols(ex)
                prices = await self.__get_same_symbol_prices(max_symbol.replace('USDT',''), ex_names, symbols)
                print(prices)
            await asyncio.sleep(1)



    async def __write_max_gap_data(self, max_symbol, max_gap, largest_long_price, largest_short_price, largest_long_ex, largest_short_ex):
        df = pd.DataFrame({
            'ts': [int(time.time())],  # リストにして単一の値を含むように修正
            'max_symol': [max_symbol],  # リストにして単一の値を含むように修正
            'max_gap': [max_gap],  # リストにして単一の値を含むように修正
            'largest_long_price': [largest_long_price],  # リストにして単一の値を含むように修正
            'largest_short_price': [largest_short_price],  # リストにして単一の値を含むように修正
            'largest_long_ex': [largest_long_ex],  # リストにして単一の値を含むように修正
            'largest_short_ex': [largest_short_ex]  # リストにして単一の値を含むように修正
        })
        if self.flg_created_file:
            df.to_csv(f'Data/max_gap.csv', mode='a', header=False)
        else:
            df.to_csv(f'Data/max_gap.csv')
            self.flg_created_file = True




    async def __get_usdt_pair_price(self):
        bids_asks = OrderobookDataList.get_latest_data('bybit', 'USDCUSDT')
        return round(float((list(bids_asks['bids'].keys())[0] + list(bids_asks['asks'].keys())[0]) / 2.0), 9)

    async def __get_same_symbol_prices(self, base_currency:str, ex_names:list, symbols:dict):
        '''
        全てのexでsymbolがbase + quoteで構成されていることが前提
        '''
        prices = {} #ex:prices ({'bids':{price:size}, 'asks':{price:size}})
        for ex in ex_names:
            if str(base_currency + Params.quotes[ex]) in symbols[ex]:
                matched_ind = symbols[ex].index(base_currency + Params.quotes[ex])
                prices[ex] = OrderobookDataList.get_latest_data(ex, symbols[ex][matched_ind])
                symbols[ex].pop(matched_ind)
        #if len(prices) == 3:
        #    print(prices)
        return prices

    async def __calc_price_gap(self, prices, usdc_usdt):
        '''
        各取引所のbid, askでmarket entryしたと想定して最大のgapが発生する組み合わせを求める。
        spread以上に乖離が開いてないと収益はマイナスになるはず。
        最も価格が高いbidと低いaskの差を計算する
        '''
        max_gap_ratio = 0
        ex_names = []
        long_ex = ''
        short_ex = ''
        #print(prices)
        bids = []
        asks = []
        for ex, price in prices.items():
            ex_names.append(ex)
            if Params.quotes[ex] == 'USDC':
                bids.append(list(price['bids'].keys())[0] / usdc_usdt)
                asks.append(list(price['asks'].keys())[0] / usdc_usdt)
            else:
                bids.append(list(price['bids'].keys())[0])
                asks.append(list(price['asks'].keys())[0])
        max_bid = max(bids) #short at max bid
        min_ask = min(asks) #long at min ask
        long_ex = ex_names[asks.index(min_ask)]
        short_ex = ex_names[bids.index(max_bid)]
        max_gap_ratio = float((max_bid - min_ask) / ((max_bid + min_ask) * 0.5))
        #if long_ex == short_ex:
        #    print(prices)
        return max_gap_ratio, long_ex, short_ex, max_bid, min_ask
        
            
