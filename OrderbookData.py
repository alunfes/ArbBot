import threading
import pandas as pd


class OrderbookData:
    def __init__(self, ex_name, symbol_name) -> None:
        self.max_data_size = 100
        self.num_recording_boards = 5
        self.ex_name = ex_name
        self.symbol_name = symbol_name
        self.bids = {} #price:size
        self.asks = {} #price:size
        self.bids_log = {} #ts:bids
        self.asks_log = {} #ts:asks
        

    def add_data(self, bids, asks, ts):
        self.bids = bids
        self.asks = asks
        self.bids_log[ts] = bids
        self.asks_log[ts] = asks
        if len(self.bids_log) >= self.max_data_size:
            self.__write_data()
            self.bids_log = {}
            self.asks_log = {}
    
    def __write_data(self):
        # bidsとasksの各tsのnum_recording_boards板分をDataFrameに保存
        df_bids = pd.DataFrame({ts: bids[:self.num_recording_boards] for ts, bids in self.bids_log.items()}).T
        df_asks = pd.DataFrame({ts: asks[:self.num_recording_boards] for ts, asks in self.asks_log.items()}).T
        # Convert to the improved format
        df_combined = pd.DataFrame(index=df_bids.index)
        for i in range(self.num_recording_boards):
            df_combined[f'bid{i}_price'] = df_bids[i].apply(lambda x: x[0] if pd.notnull(x) else None)
            df_combined[f'bid{i}_volume'] = df_bids[i].apply(lambda x: x[1] if pd.notnull(x) else None)
            df_combined[f'ask{i}_price'] = df_asks[i].apply(lambda x: x[0] if pd.notnull(x) else None)
            df_combined[f'ask{i}_volume'] = df_asks[i].apply(lambda x: x[1] if pd.notnull(x) else None)
        # Write to CSV
        df_combined.index.name = 'ts'
        mode = 'a' if self.flg_created_file else 'w'
        header = not self.flg_created_file
        df_combined.to_csv(f'Data/depth/{self.ex_name}_{self.symbol}_depth.csv', mode=mode, header=header)
        if not self.flg_created_file:
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
    

        
    

    

