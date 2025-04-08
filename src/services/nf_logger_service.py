from datetime import datetime

from services.logger_service import LoggerService

class NotaFiscalLoggerService:
    def __init__(self):
        self.logs = []
        self.errors = []
        self.logger_service = LoggerService()
        
    def clear_monitoring(self):
        if self.logs:
            self.logs.clear()
        if self.errors:  
            self.errors.clear()
    
    def track_monitoring(self, msg: str):
        self.track_log(msg)
        self.track_error(msg)
            
    def track_error(self, msg_or_msgs):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # 2025-03-12 14:30:45.456
        if isinstance(msg_or_msgs, list):
            for msg in msg_or_msgs:
                self.track_error(msg)  # Recursively handle each item
        else:
            self.errors.append(f"{timestamp} -> {msg_or_msgs}")  # Assume it's a single string
            self.logger_service.error(msg_or_msgs)
            #print(f"{timestamp} -> {msg_or_msgs}")
    
    def track_log(self, msg_or_msgs):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # 2025-03-12 14:30:45.456
        if isinstance(msg_or_msgs, list):
            for msg in msg_or_msgs:
                self.track_log(msg)  # Recursively handle each item
        else:
            self.logs.append(f"{timestamp} -> {msg_or_msgs}")  # Assume it's a single string
            self.logger_service.info(msg_or_msgs)
            #print(f"{timestamp} -> {msg_or_msgs}")
        
    def separator(self):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # 2025-03-12 14:30:45.456
        self.logs.append(f"{timestamp} -> ----------------------------------------------------")
        self.logger_service.info("----------------------------------------------------")
        #print(f"{timestamp} -> ----------------------------------------------------")