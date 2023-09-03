import asyncio
import pandas as pd

from collections import Counter
from OrderbookData import OrderbookData, OrderobookDataList
from ApexWS import ApexWS
from BybitWS import BybitWS
from ApexAPI import ApexAPI
from BybitAPI import BybitAPI
from Bot import Bot
from Params import Params

class Application:
    def __init__(self, target_ex_names):
        self.target_ex_names = target_ex_names
        self.all_tickers = {}
        
        
    async def start(self):
        Params.initialize()
        OrderobookDataList.initialize()
        await self.__get_all_tickers()
        target_base_currencies = self.__detect_common_target_tickers(self.all_tickers.copy())
        #generate ws for target ex
        ws_instances = []
        ex_ws_funcs = {'apex':ApexWS(target_base_currencies, 5), 'bybit':BybitWS(target_base_currencies, 5)}
        ws_tasks = [ex_ws_funcs[ex].start() for ex in self.target_ex_names]
        bot = Bot()
        await asyncio.gather(
            *ws_tasks,
            bot.start(),
            )





    async def __get_all_tickers(self):
        self.all_tickers = {} #ex_name:tickers
        for ex in self.target_ex_names:
            if ex == 'apex':
                api = ApexAPI()
                self.all_tickers['apex'] = await api.get_tickers()
                pd.DataFrame(self.all_tickers['apex']).to_csv('Data/tickers/apex_tickers.csv')
            elif ex == 'bybit':
                api = BybitAPI()
                self.all_tickers['bybit'] = await api.get_tickers()
                pd.DataFrame(self.all_tickers['bybit']).to_csv('Data/tickers/bybit_tickers.csv')
            else:
                print('Invalid exchange !', ex)

    def __detect_common_target_tickers(self, all_tickers):
        # Find common base currency
        combined_base = []
        for ex in self.target_ex_names:
            combined_base.extend(all_tickers[ex]['base_currency'])
        common_base_currency = [item for item, count in Counter(combined_base).items() if count > 1]
        pd.DataFrame(common_base_currency).to_csv('Data/tickers/common_base_currency.csv')
        return common_base_currency





if __name__ == '__main__':
    app = Application(['apex','bybit'])
    asyncio.run(app.start())