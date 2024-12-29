from time import time, sleep

class rateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.tokens = max_calls
        self.last_checked = time()
    

    def acquire(self):
        current_time = time()
        elapsed = current_time - self.last_checked

        self.tokens += elapsed * (self.max_calls / self.period)
        if self.tokens > self.max_calls:
            self.tokens = self.max_calls
        
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        else:
            print("API rate limit reached. Waiting for next period.")
            sleep(self.period / self.max_calls)
            return self.acquire()