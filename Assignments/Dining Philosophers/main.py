import random
import threading
import time

from kazoo.client import KazooClient

NUM_PHILOSOPHERS = 5  # Number of philosophers
MAX_MEALS = 10  # Maximum number of meals each philosopher can have

# ANSI escape sequences for colors
COLOR_RED = '\033[91m'
COLOR_GREEN = '\033[92m'
COLOR_YELLOW = '\033[93m'
COLOR_RESET = '\033[0m'

class Philosopher:
    def __init__(self, philosopher_id, zk_client):
        """
        Initializes a Philosopher object.

        Args:
            philosopher_id (int): The ID of the philosopher.
            zk_client (KazooClient): The ZooKeeper client instance.
        """
        self.philosopher_id = philosopher_id
        self.left_fork_id = philosopher_id % NUM_PHILOSOPHERS
        self.right_fork_id = (philosopher_id + 1) % NUM_PHILOSOPHERS
        self.zk_client = zk_client

    def think(self):
        """
        Represents the thinking behavior of the philosopher.
        """
        random_thinking_time = random.uniform(0, 3)
        print(f"Philosopher {self.philosopher_id}: {COLOR_YELLOW}thinking{COLOR_RESET} for {random_thinking_time:.2f} seconds")
        time.sleep(random_thinking_time)

    def eat(self):
        """
        Represents the eating behavior of the philosopher.
        """
        random_eating_time = random.uniform(0, 2)
        print(f"Philosopher {self.philosopher_id}: {COLOR_GREEN}eating{COLOR_RESET} for {random_eating_time:.2f} seconds")
        time.sleep(random_eating_time)

    def run(self):
        """
        Runs the main loop for the philosopher, alternating between thinking and eating.
        """
        for _ in range(MAX_MEALS):
            self.think()

            while not self.acquire_forks():
                # Wait until locks are acquired
                time.sleep(0.1)

            self.eat()

            self.release_forks()
            print(f"Philosopher {self.philosopher_id}: {COLOR_RED}finished eating{COLOR_RESET}")

    def acquire_forks(self):
        """
        Acquires locks for the left and right forks.

        Returns:
            bool: True if locks are acquired successfully, False otherwise.
        """
        fork_paths = []

        try:
            # Create lock for each fork
            for fork_id in (self.left_fork_id, self.right_fork_id):
                fork_path = f"/fork_resource/{fork_id}"
                self.zk_client.create(fork_path)
                fork_paths.append(fork_path)
        except:
            # Delete created locks if an exception occurs
            for fork_path in fork_paths:
                self.zk_client.delete(fork_path)
            return False

        return True

    def release_forks(self):
        """
        Releases the locks for the left and right forks.
        """
        self.zk_client.delete(f"/fork_resource/{self.left_fork_id}")
        self.zk_client.delete(f"/fork_resource/{self.right_fork_id}")


def main():
    zk_client = KazooClient(hosts='localhost:2181')
    zk_client.start()

    # Create the "forks" path if it doesn't exist
    zk_client.ensure_path("/fork_resource")

    # Delete any existing forks
    fork_children = zk_client.get_children("/fork_resource")
    for child in fork_children:
        zk_client.delete("/fork_resource/" + child)

    philosophers = [Philosopher(i, zk_client) for i in range(NUM_PHILOSOPHERS)]

    # Create philosopher threads
    philosopher_threads = [threading.Thread(target=philosopher.run) for philosopher in philosophers]

    # Start philosopher threads
    for thread in philosopher_threads:
        thread.start()

    # Wait for philosopher threads to complete
    for thread in philosopher_threads:
        thread.join()

if __name__ == "__main__":
    main()
