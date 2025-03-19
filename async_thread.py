import asyncio
import threading
import queue
import time
import logging

# Configure basic logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s: %(message)s')

async def producer(named_queue, task_id):
    """Asynchronously puts items into the queue."""
    for i in range(50):
        item = f"Task {task_id} - Item {i}"
        # await asyncio.sleep(0.5)  # Simulate asynchronous work
        named_queue.put(item)
        logging.info(f"Producer {task_id} put: {item}")
    # Signal that this producer is done by putting a sentinel.
    named_queue.put(None)

def consumer(named_queue, num_producers):
    """Consumes items from the queue in a separate thread."""
    finished_producers = 0
    while finished_producers < num_producers:
        try:
            item = named_queue.get(timeout=1)  # Non-blocking get with timeout
            if item is None:
                # A producer has signaled it's done.
                finished_producers += 1
                logging.info("Consumer received a sentinel (producer finished).")
            else:
                logging.info(f"Consumer received: {item}")
            named_queue.task_done()  # Signal that the item is processed
        except queue.Empty:
            # If queue is empty, wait a bit more.
            time.sleep(0.1)
    logging.info("Consumer finished processing all items.")

def run_async_in_thread(loop, coro):
    """Runs an asynchronous coroutine in a thread."""
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

def start_producer_thread(task_id, named_queue):
    """Starts an asynchronous producer in a new thread."""
    loop = asyncio.new_event_loop()
    coro = producer(named_queue, task_id)
    thread = threading.Thread(target=run_async_in_thread, args=(loop, coro), name=f"ProducerThread-{task_id}")
    thread.start()
    return thread

def main():
    """Main function to orchestrate producers and consumer."""
    num_producers = 4
    # Create a single shared queue.
    named_queue = queue.Queue()
    producer_threads = []

    # Start multiple producer threads.
    for i in range(num_producers):
        producer_thread = start_producer_thread(i, named_queue)
        producer_threads.append(producer_thread)

    # Start the consumer thread.
    consumer_thread = threading.Thread(target=consumer, args=(named_queue, num_producers), name="ConsumerThread")
    consumer_thread.start()

    # Wait for all producer threads to finish.
    for producer_thread in producer_threads:
        producer_thread.join()

    # Wait for the consumer thread to finish.
    consumer_thread.join()

    logging.info("All tasks completed.")

if __name__ == "__main__":
    main()