import threading
import pandas as pd
import asyncio


class OrderbookData:
    def __init__(self, ex_name, symbol_name, flg_write_data) -> None:
        self.max_data_size = 50
        self.num_recording_boards = 5
        self.ex_name = ex_name
        self.symbol_name = symbol_name
        self.flg_write_data = flg_write_data
        self.bids = {} #price:size
        self.asks = {} #price:size
        self.bids_log = {} #ts:bids
        self.asks_log = {} #ts:asks
        self.flg_created_file = False
        
    '''
    def fire_and_forget(func):
        def wrapper(*args, **kwargs):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_in_executor(None, func, *args, *kwargs)
        return wrapper
    '''
    
    
    def fire_and_forget(func):
        def wrapper(*args, **kwargs):
            # 非同期タスクを作成し、スレッドプールを使用
            loop = asyncio.get_event_loop()
            future = loop.run_in_executor(None, func, *args, *kwargs)
            # エラーハンドリングを追加
            def handle_callback(future):
                try:
                    future.result()  # タスクの実行結果を取得
                except Exception as e:
                    # ここでエラーハンドリングを行う (例: ログに記録)
                    print(f"An error occurred: {e}")
            future.add_done_callback(handle_callback)
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


    def get_data(self):
        return {'bids':self.bids.copy(), 'asks':self.asks.copy()}
    
    
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
        if self.flg_write_data:
            if self.flg_created_file:
                df_combined.to_csv(f'Data/depth/{self.ex_name}_{self.symbol_name}_depth.csv', mode='a', header=False)
            else:
                df_combined.to_csv(f'Data/depth/{self.ex_name}_{self.symbol_name}_depth.csv')
                self.flg_created_file = True


class OrderobookDataList:
    @classmethod
    def initialize(cls, flg_write_data):
        '''
        一番最初に一回だけ実行
        '''
        cls.orderbook_data_list = {} #ex_name-symbol_name:
        cls.ex_names = []
        cls.symbol_names = {}#ex_name:symbol_list
        cls._locks = {} #ex-symbol:lock,   dictキーにアクセスする時間がかかるけどlockは個別の変数ごとに並列ができる。
        cls.flg_write_data = flg_write_data
        cls.lock = threading.RLock()
        cls.tmp_flg = 0


    @classmethod
    def setup_new_ex_symbol(cls, ex_name, symbol_name):
        '''
        新しいsymbolのデータ取得を開始するときに一回だけ実行
        '''
        with cls.lock:
            cls.orderbook_data_list[ex_name+'-'+symbol_name] = OrderbookData(ex_name, symbol_name, cls.flg_write_data)
            if ex_name not in cls.ex_names:
                cls.ex_names.append(ex_name)
                cls.symbol_names[ex_name] = [symbol_name]
                cls._locks[ex_name+'-'+symbol_name] = threading.RLock()
            else:
                cls.symbol_names[ex_name].append(symbol_name)
                cls._locks[ex_name+'-'+symbol_name] = threading.RLock()
            
    @classmethod
    def add_data(cls, ex_name, symbol_name, bids, asks, ts):
        #with cls.lock:
        with cls._locks[ex_name+'-'+symbol_name]:
            cls.orderbook_data_list[ex_name+'-'+symbol_name].add_data(bids, asks, ts)
            if min(asks.keys()) < max(bids.keys()):
                print('************************************************************')
                print('max bid price is higher than min ask price!')
                print(ex_name, symbol_name)
                print('bids:', bids)
                print('asks:', asks)
                print('************************************************************')
                cls.tmp_flg += 1
                if cls.tmp_flg == 5:
                    print('kita')


    
    @classmethod
    def get_latest_data(cls, ex_name, symbol_name):
        #with cls.lock:
        with cls._locks[ex_name+'-'+symbol_name]:
            return cls.orderbook_data_list[ex_name+'-'+symbol_name].get_data()
        
    @classmethod
    def get_ex_names(cls):
        with cls.lock:
            return cls.ex_names.copy()
    
    @classmethod
    def get_symbols(cls, ex_name):
        with cls.lock:
            return cls.symbol_names[ex_name].copy()
        
    

    

        
    

    

