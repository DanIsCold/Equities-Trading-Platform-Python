import time
import threading
import queue

class APIRateLimiter:
    def __init__(self, max_calls_per_minute=200):
        self.max_calls_per_minute = max_calls_per_minute
        self.call_queue = queue.Queue()
        self.lock = threading.Lock()
        self.call_times = []  # Keep track of call times
        self.stop_flag = False

    def add_to_queue(self, request_data):
        # Check if the call limit has been reached
        current_time = time.time()
        self.call_times = [t for t in self.call_times if current_time - t < 60]  # Remove timestamps older than 60 seconds

        if len(self.call_times) >= self.max_calls_per_minute:
            print("Rate limit reached, waiting...")
            return False  # Reject the request to prevent adding to the queue

        # Otherwise, add the request to the queue and the timestamp
        self.call_times.append(current_time)
        self.call_queue.put(request_data)
        print(f"API call added to the queue. Current queue size: {self.call_queue.qsize()}")
        return True

    def process_queue(self):
        while not self.stop_flag:
            if self.call_queue.empty():
                continue

            request_data = self.call_queue.get()
            if request_data is None:  # Stop signal
                break

            # Process the API call (for example, calling the fetch_market_data function)
            self.process_request(request_data)
            self.call_queue.task_done()

            # Delay to avoid overloading the API if needed
            time.sleep(0.1)  # Adjust sleep time if necessary

    def process_request(self, request_data):
        # Replace this with actual API call logic, e.g., fetch_market_data
        print(f"Processing request: {request_data}")

    def stop(self):
        self.stop_flag = True

    def run(self):
        while not self.stop_flag:
            # Add API calls to the queue with rate limiting in place
            request_data = {"func": self.process_request, "args": (), "kwargs": {}}
            if self.add_to_queue(request_data):
                threading.Thread(target=self.process_queue).start()
            time.sleep(0.5)  # Delay before attempting to add another API call
