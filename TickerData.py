class TickerData:
    @classmethod
    def initialize(cls):
        cls.tickers = {} #ex_name:ticker list
    
    @classmethod
    def add_ticker(cls, ex_name, tickers):
        cls.tickers[ex_name]= tickers
    
    @classmethod
    def add_all_ticker(cls):
        return cls.tickers.copy()
    