import threading

class DisplaySystemMessage:
    @classmethod
    def initialize(cls):
        cls.lock = threading.RLock()
    
    @classmethod
    def display_error(cls, class_name, error_message):
        print('*****************Error****************')
        print('Class=', class_name)
        print(error_message)
        print('**************************************')
    
