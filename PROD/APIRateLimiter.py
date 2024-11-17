import time
from collections import deque
from threading import Lock


class APIRateLimiter:
    def __init__(self, max_calls_per_minute):
        """
        Initializes the API limiter with a maximum number of calls per minute.
        
        :param max_calls_per_minute: The maximum number of API calls allowed per minute.
        """
        self.max_calls_per_minute = max_calls_per_minute
        self.call_times = deque()  # Stores timestamps of API calls
        self.lock = Lock()  # Thread-safety for managing queue
    
    def add_call(self):
        """
        Adds a new API call to the queue. Blocks if the queue exceeds the limit.
        """
        with self.lock:
            current_time = time.time()
            
            # Remove timestamps older than 60 seconds
            while self.call_times and current_time - self.call_times[0] > 60:
                self.call_times.popleft()
            
            # Check if the number of API calls in the last 60 seconds exceeds the limit
            while len(self.call_times) >= self.max_calls_per_minute:
                # Wait for the oldest call to fall outside the 60-second window
                time_to_wait = 60 - (current_time - self.call_times[0])
                time.sleep(time_to_wait)
                current_time = time.time()
                # Recheck and clean outdated calls
                while self.call_times and current_time - self.call_times[0] > 60:
                    self.call_times.popleft()
            
            # Add the current call timestamp to the queue
            self.call_times.append(current_time)
            print(f"API call added at {current_time}. Current queue size: {len(self.call_times)}")
