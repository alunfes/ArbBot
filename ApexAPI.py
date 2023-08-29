from apexpro.constants import APEX_HTTP_MAIN
from apexpro.http_public import HttpPublic
from apexpro import HTTP

import asyncio
import aiohttp
import time
import pandas as pd



class ApexAPI:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ApexAPI, cls).__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self.__initialized:
            return
        self.__initialized = True
        self.max_num_donwload_data = 1500
        self.client = HttpPublic(APEX_HTTP_MAIN)

    
    '''
    {'data': {'ETHUSDC': [{'s': 'ETHUSDC', 'i': '1', 't': 1690009500000, 'c': '1892.1', 'h': '1892.1', 'l': '1892', 'o': '1892', 'v': '4.74', 'tr': '8968.55'}, {'s': 'ETHUSDC', 'i': '1', 't': 1690009560000, 'c': '1892.1', 'h': '1892.1', 'l': '1892.1', 'o': '1892.1', 'v': '0', 'tr': '0'}, {'s': 'ETHUSDC', 'i': '1', 't': 1690009620000, 'c': '1892.95', 'h': '1893.1', 'l': '1892.1', 'o': '1892.1', 'v': '2.25', 'tr': '4259.157'}, {'s': 'ETHUSDC', 'i': '1', 't': 1690009680000, 'c': '1892.95', 'h': '1893', 'l': '1892.95', 'o': '1892.95', 'v': '6.18', 'tr': '11698.4485'}}
    '''
    async def get_klines(self, symbol, since_ts:int, till_ts:int, interval:int):
        client = HttpPublic(APEX_HTTP_MAIN)
        ohlc_list = []
        end_ts = since_ts + (self.max_num_donwload_data * interval * 60)
        pre_ts = 0
        while True:
            res = client.klines(
                symbol = symbol,
                interval = 1,
                start = since_ts,
                end = end_ts
                )
            if len(res['data']) > 0 and res['data'][symbol][-1]['t'] / 1000 < till_ts and pre_ts < res['data'][symbol][-1]['t']:
                ohlc_list.extend(res['data'][symbol])
                since_ts = int(res['data'][symbol][-1]['t'] / 1000) + interval
                end_ts = since_ts + (self.max_num_donwload_data * interval * 60)
                pre_ts = res['data'][symbol][-1]['t']
            else:
                break
            await asyncio.sleep(0.1)
        return ohlc_list
    

    async def get_tickers(self):
        url = 'https://pro.apex.exchange/api/v1/symbols'
        symbols = []
        base_currency = []
        quote_currency = []
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp = await resp.json()
                for ticker in resp['data']["perpetualContract"]:
                    if ticker["enableTrade"]:
                        symbols.append(ticker['crossSymbolName'])
                        base_currency.append(ticker['underlyingCurrencyId'])
                        quote_currency.append(ticker['settleCurrencyId'])
        return {'symbols':symbols, 'base_currency':base_currency, 'quote_currency':quote_currency}



if __name__ == '__main__':
    apex = ApexAPI()
    ohlc = asyncio.run(apex.get_klines('BCHUSDC', int(time.time()-60 * 120), int(time.time()), 1))
    df = pd.DataFrame(ohlc)
    print(df)