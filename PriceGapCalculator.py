import threading
import asyncio

from OrderbookData import OrderobookDataList
from Params import Params

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
    def __init__(self) -> None:
        pass
    
    async def start(self):
        await asyncio.sleep(60) #wait till bid/ask data of all target symbols become available\
        self.ex_names = OrderobookDataList.get_ex_names()
        self.symbols = {} #ex:symbols
        for ex in self.ex_names:
            self.symbols[ex] = OrderobookDataList.get_symbols(ex)
        while True:
            #毎回listから削除しながら同一銘柄の価格差を検証するためのリストをコピーして作成する。
            ex_names = self.ex_names.copy()
            symbols = {}
            for ex in self.ex_names:
                symbols[ex] = OrderobookDataList.get_symbols(ex)
            usdc_usdt = await self.__get_usdt_pair_price()
            await asyncio.sleep(1)

    async def __get_usdt_pair_price(self):
        return OrderobookDataList.get_latest_data('bybit', 'USDCUSDT')

    async def __get_same_symbol_prices(self, base_quote:str, ex_names:list, symbols:dict):
        
        for ex in ex_names:

            OrderobookDataList.get_latest_data(ex, )


