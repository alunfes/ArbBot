import threading
import pandas as pd
import asyncio


class OrderbookData:
    def __init__(self, ex_name, symbol_name) -> None:
        self.max_data_size = 50
        self.num_recording_boards = 5
        self.ex_name = ex_name
        self.symbol_name = symbol_name
        self.bids = {} #price:size
        self.asks = {} #price:size
        self.bids_log = {} #ts:bids
        self.asks_log = {} #ts:asks
        self.flg_created_file = False
        

    def fire_and_forget(func):
        def wrapper(*args, **kwargs):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_in_executor(None, func, *args, *kwargs)
        return wrapper

    def add_data(self, bids, asks, ts):
        self.bids = bids
        self.asks = asks
        self.bids_log[ts] = bids
        self.asks_log[ts] = asks
        if len(self.bids_log) >= self.max_data_size:
            self.__write_data(self.bids_log.copy(), self.asks_log.copy())
            self.bids_log = {}
            self.asks_log = {}
    
    @fire_and_forget
    def __write_data(self, bids_log, asks_log):
        # bidsとasksの各tsのnum_recording_boards板分をDataFrameに保存
        rows = []
        for timestamp, values in bids_log.items():
            row = {'ts': timestamp}
            for i, (price, size) in enumerate(values.items(), start=1):
                row[f'bid_price{i}'] = price
                row[f'bid_size{i}'] = size
            rows.append(row)
        df_bids = pd.DataFrame(rows)
        rows = []
        for timestamp, values in asks_log.items():
            row = {'ts': timestamp}
            for i, (price, size) in enumerate(values.items(), start=1):
                row[f'ask_price{i}'] = price
                row[f'ask_size{i}'] = size
            rows.append(row)
        df_asks = pd.DataFrame(rows)
        # Convert to the improved format
        df_combined = pd.concat([df_bids, df_asks], axis=1)
        # Write to CSV
        if self.flg_created_file:
            df_combined.to_csv(f'Data/depth/{self.ex_name}_{self.symbol_name}_depth.csv', mode='a', header=False)
        else:
            df_combined.to_csv(f'Data/depth/{self.ex_name}_{self.symbol_name}_depth.csv')
            self.flg_created_file = True


class OrderobookDataList:
    @classmethod
    def initialize(cls):
        '''
        一番最初に一回だけ実行
        '''
        cls.orderbook_data_list = {} #ex_name-symbol_name:
        cls.lock = threading.RLock()

    @classmethod
    def setup_new_ex_symbol(cls, ex_name, symbol_name):
        '''
        新しいsymbolのデータ取得を開始するときに一回だけ実行
        '''
        with cls.lock:
            cls.orderbook_data_list[ex_name+'-'+symbol_name] = OrderbookData(ex_name, symbol_name)

    @classmethod
    def add_data(cls, ex_name, symbol_name, bids, asks, ts):
        with cls.lock:
            cls.orderbook_data_list[ex_name+'-'+symbol_name].add_data(bids, asks, ts)
            print(bids)
            print(asks)
    
    @classmethod
    def get_all_data(cls):
        with cls.lock:
            return cls.orderbook_data_list.copy()
    

        
    

    

