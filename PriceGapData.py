import threading

class PriceGapData:
    @classmethod
    def initialize(cls):
        cls._max_gap_ratio = {} #symbol:gap ratio
        cls._max_price_ex = {} #symbol:max price ex
        cls._min_price_ex = {} #symbol:min price ex
        cls._min_ask = {}
        cls._max_bid = {}
        cls._max_gap_symbol = ''
        cls._largest_gap_ratio = -999.99
        cls._largest_gap_bid = 0
        cls._largest_gap_ask = 0
        cls._largest_gap_bid_ex = ''
        cls._largest_gap_ask_ex = ''
        cls._lock = threading.RLock()

    @classmethod
    def add_gap_price(cls, symbol, gap_ratio, max_price_ex, min_price_ex, max_bid, min_ask):
        with cls._lock:
            cls._max_gap_ratio[symbol] = gap_ratio
            cls._max_price_ex[symbol] = max_price_ex
            cls._min_price_ex[symbol] = min_price_ex
            cls._max_bid[symbol] = max_bid
            cls._min_ask[symbol] = min_ask
            #record max gap symbol data
            max_symbol = max(cls._max_gap_ratio, key=cls._max_gap_ratio.get)
            cls._largest_gap_ratio = cls._max_gap_ratio[max_symbol]
            cls._max_gap_symbol = max_symbol
            cls._largest_gap_bid = cls._max_bid[max_symbol]
            cls._largest_gap_ask = cls._min_ask[max_symbol]
            cls._largest_gap_bid_ex = cls._max_price_ex[max_symbol]
            cls._largest_gap_ask_ex = cls._min_price_ex[max_symbol]


    @classmethod
    def get_max_gap_data(cls):
        with cls._lock:
            return cls._max_gap_symbol, cls._largest_gap_ratio, cls._largest_gap_ask, cls._largest_gap_bid, cls._largest_gap_bid_ex, cls._largest_gap_ask_ex
    