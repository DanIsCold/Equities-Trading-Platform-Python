import time
from queue import Queue
from threading import Thread

class APIRateLimiter:
    def __init__(self):
        self.max_calls = 200
        self.call_times = []  # Store timestamps of API calls
        self.queue = Queue()  # Queue for API requests


    def proc_queue(self):
        """Process requests from the queue."""
        while True:
            request = self.queue.get()  # Get a request from the queue
            if request is None:  # Stop the thread if a stop signal is received
                break

            # Throttle API calls
            while len(self.call_times) >= self.max_calls:
                time.sleep(0.1)  # Wait a short time before checking again
                now = time.time()
                # Remove timestamps older than 60 seconds
                self.call_times = [t for t in self.call_times if now - t < 60]

            # Make the API request
            self._make_request(request)

            # Record the timestamp of this request
            self.call_times.append(time.time())
            self.queue.task_done()

    def add_request(self, request):
        """Add a request to the queue."""
        self.queue.put(request)

    def start(self):
        """Start the thread to process the queue."""
        self.thread = Thread(target=self.proc_queue)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        """Stop the thread processing the queue."""
        self.queue.put(None)
        self.thread.join()


# Example usage
if __name__ == "__main__":
    rate_limiter = APIRateLimiter(max_calls_per_minute=200)
    rate_limiter.start()

    # Simulate adding API requests to the queue
    for i in range(300):
        rate_limiter.add_request(f"Request #{i + 1}")

    # Wait for all requests to be processed
    rate_limiter.queue.join()
    rate_limiter.stop()
