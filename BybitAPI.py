import asyncio
import aiohttp
import time
import pandas as pd


class BybitAPI:
    def __init__(self) -> None:
        self.max_num_donwload_data = 200
    
    
    async def get_tickers(self):
        '''
        {"markets":{"CELO-USD":{"market":"CELO-USD","status":"ONLINE","baseAsset":"CELO","quoteAsset":"USD","stepSize":"1","tickSize":"0.001","indexPrice":"0.4173","oraclePrice":"0.4167","priceChange24H":"0.000086","nextFundingRate":"0.0000098246","nextFundingAt":"2023-06-14T05:00:00.000Z","minOrderSize":"10","type":"PERPETUAL","initialMarginFraction":"0.2","maintenanceMarginFraction":"0.05","transferMarginFraction":"0.004204","volume24H":"2677351.970000","trades24H":"1849","openInterest":"624888","incrementalInitialMarginFraction":"0.02","incrementalPositionSize":"17700","maxPositionSize":"355000","baselinePositionSize":"35500","assetResolution":"1000000","syntheticAssetId":"0x43454c4f2d36000000000000000000"},"LINK-USD":{"market":"LINK-USD","status":"ONLINE","baseAsset":"LINK","quoteAsset":"USD","stepSize":"0.1","tickSize":"0.001","indexPrice":"5.3934","oraclePrice":"5.3818","priceChange24H":"0.213470","nextFundingRate":"0.0000100141","nextFundingAt":"2023-06-14T05:00:00.000Z","minOrderSize":"1","type":"PERPETUAL","initialMarginFraction":"0.10","maintenanceMarginFraction":"0.05","transferMarginFraction":"0.008147","volume24H":"2699917.002400","trades24H":"3135","openInterest":"316078.7","incrementalInitialMarginFraction":"0.02","incrementalPositionSize":"14000","maxPositionSize":"700000","baselinePositionSize":"70000","assetResolution":"10000000","syntheticAssetId":"0x4c494e4b2d37000000000000000000"},
        '''
        url = 'https://api.bybit.com//v5/market/instruments-info?category=linear'
        symbols = []
        base_currency = []
        quote_currency = []
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp = await resp.json()
                for details in resp['result']['list']:
                    if details['status'] == 'Trading' and details['quoteCoin'] == 'USDT':
                        symbols.append(details['symbol'])
                        base_currency.append(details['baseCoin'])
                        quote_currency.append('USDT')
        return {'symbols':symbols, 'base_currency':base_currency, 'quote_currency':quote_currency}




if __name__ == '__main__':
    apex = BybitAPI()
    tickers = asyncio.run(apex.get_tickers())
    df = pd.DataFrame(tickers)
    print(df)