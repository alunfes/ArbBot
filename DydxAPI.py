import pandas as pd
import asyncio
from dydx3 import Client
from web3 import Web3


'''
~/.local/share/virtualenvs/DydxBot-gghsreHu/lib/python3.11/site-packages/parsimonious/expressions.pyのfrom inspect import getargspecをgetfullargspecに修正
'''
class DydxAPI:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance
    
    def __init__(self):
        self.public_client = Client(host='https://api.dydx.exchange')

    async def get_tickers(self):
        '''
        {'markets': {'CELO-USD': {'market': 'CELO-USD', 'status': 'ONLINE', 'baseAsset': 'CELO', 'quoteAsset': 'USD', 'stepSize': '1', 'tickSize': '0.001', 'indexPrice': '0.4299', 'oraclePrice': '0.4308', 'priceChange24H': '-0.002917', 'nextFundingRate': '-0.0000067216', 'nextFundingAt': '2023-09-09T03:00:00.000Z', 'minOrderSize': '10', 'type': 'PERPETUAL', 'initialMarginFraction': '0.2', 'maintenanceMarginFraction': '0.05', 'transferMarginFraction': '0.006955', 'volume24H': '1457258.565000', 'trades24H': '1983', 'openInterest': '556406', 'incrementalInitialMarginFraction': '0.02', 'incrementalPositionSize': '17700', 'maxPositionSize': '355000', 'baselinePositionSize': '35500', 'assetResolution': '1000000', 'syntheticAssetId': '0x43454c4f2d36000000000000000000'}, 'LINK-USD': {'market': 'LINK-USD', 'status': 'ONLINE', 'baseAsset': 'LINK', 'quoteAsset': 'USD', 'stepSize': '0.1', 'tickSize': '0.001', 'indexPrice': '6.2656', 'oraclePrice': '6.2734', 'priceChange24H': '-0.086343', 'nextFundingRate': '-0.0000009271', 'nextFundingAt': '2023-09-09T03:00:00.000Z', 'minOrderSize': '1', 'type': 'PERPETUAL', 'initialMarginFraction': '0.10', 'maintenanceMarginFraction': '0.05', 'transferMarginFraction': '0.003151', 'volume24H': '3235232.018800', 'trades24H': '3991', 'openInterest': '459732.6', 'incrementalInitialMarginFraction': '0.02', 'incrementalPositionSize': '14000', 'maxPositionSize': '700000', 'baselinePositionSize': '70000', 'assetResolution': '10000000', 'syntheticAssetId': '0x4c494e4b2d37000000000000000000'}, 'DOGE-USD': {'market': 'DOGE-USD', 'status': 'ONLINE', 'baseAsset': 'DOGE', 'quoteAsset': 'USD', 'stepSize': '10', 'tickSize': '0.0001', 'indexPrice': '0.0637', 'oraclePrice': '0.0636', 'priceChange24H': '0.000068', 'nextFundingRate': '0.0000125000', 'nextFundingAt': '2023-09-09T03:00:00.000Z', 'minOrderSize': '100', 'type': 'PERPETUAL', 'initialMarginFraction': '0.10', 'maintenanceMarginFraction': '0.05', 'transferMarginFraction': '0.001502', 'volume24H': '2907576.673000', 'trades24H': '1787', 'openInterest': '47082370', 'incrementalInitialMarginFraction': '0.02', 'incrementalPositionSize': '1400000', 'maxPositionSize': '70000000', 'baselinePositionSize': '7000000', 'assetResolution': '100000', 'syntheticAssetId': '0x444f47452d35000000000000000000'}, 
        '''
        res = self.public_client.public.get_markets()
        tickers = []
        bases = []
        quotes = []
        for details in res.data['markets'].values():
            if details['status'] == 'ONLINE':
                tickers.append(details['market'])
                bases.append(details['baseAsset'])
                quotes.append(details['quoteAsset'])
        return {'symbols':tickers, 'base_currency':bases, 'quote_currency':quotes}


if __name__ == '__main__':
    api = DydxAPI()
    res = api.get_tickers()
    res.to_csv('./Data/ticker/tickers.csv')
    print(res)