import threading

class PriceGapData:
    @classmethod
    def initialize(cls):
        cls._max_gap_ratio = {} #symbol:gap ratio
        cls._long_ex = {} #symbol:max price ex
        cls._short_ex = {} #symbol:min price ex
        cls._long_price = {}
        cls._short_price = {}
        cls._max_gap_symbol = ''
        cls._largest_gap_ratio = -999.99
        cls._largest_gap_long_price = 0
        cls._largest_gap_short_price = 0
        cls._largest_gap_long_ex = ''
        cls._largest_gap_short_ex = ''
        cls._lock = threading.RLock()

    @classmethod
    def add_gap_price(cls, symbol, max_gap_ratio, long_ex, short_ex, max_bid, min_ask):
        with cls._lock:
            cls._max_gap_ratio[symbol] = max_gap_ratio
            cls._long_ex[symbol] = long_ex
            cls._short_ex[symbol] = short_ex
            cls._long_price[symbol] = min_ask
            cls._short_price[symbol] = max_bid
            #record max gap symbol data
            max_symbol = max(cls._max_gap_ratio, key=cls._max_gap_ratio.get)
            cls._largest_gap_ratio = cls._max_gap_ratio[max_symbol]
            cls._max_gap_symbol = max_symbol
            cls._largest_gap_long_price = cls._long_price[max_symbol]
            cls._largest_gap_short_price = cls._short_price[max_symbol]
            cls._largest_gap_long_ex = cls._long_ex[max_symbol]
            cls._largest_gap_short_ex = cls._short_ex[max_symbol]
            


    @classmethod
    def get_max_gap_data(cls):
        with cls._lock:
            return cls._max_gap_symbol, cls._largest_gap_ratio, cls._largest_gap_long_price, cls._largest_gap_short_price, cls._largest_gap_long_ex, cls._largest_gap_short_ex
    