"""
    Consistent Hashing Distributed Hash Table (DHT)
    Author: Sashank Silwal
    Date: 30/03/2023

"""
import random
from pymemcache.client.base import Client
from pymemcache.client.murmur3 import murmur3_32
import faker

class Node:
    def __init__(self, name: str, ip: str, port: int):
        """Create a new node with the given name, IP address, and port."""
        try: 
            self.name = name
            self.ip = ip
            self.port = port    
            self.data = {}
            self.client = Client((ip, port))
            self.client.set('name', name)
        except ConnectionRefusedError:
            print("Connection refused")
            
    
class ConsistentHashRing:
    def __init__(self, nodes, replicas: int = 2):
        """
        Create a new consistent hash ring with the given nodes and replicas.

        :param nodes: a list of nodes to add to the ring
        """
        self.replicas = replicas
        self.hash_ring = {}
        self.nodes = []

        # Add each node to the hash ring
        for node in nodes:
            self.add(node)

    def add(self, node: Node):
        """
        Add a new node to the hash ring.

        :param node: the node to add to the ring
        """
        # Add the node to the list of nodes
        self.nodes.append(node)

        # add the node to the hash ring
        key = f"{node.name}"
        hash_val = self.get_hash(key)
        hash_val = self.get_hash(node.name)
        self.hash_ring[hash_val] = node

        # Sort the keys in the hash ring
        self.nodes = sorted(self.nodes, key=lambda x: self.get_hash(x.name))


    def remove(self, node):
        """
        Remove a node from the hash ring and reassign the values from the removed node to the next node on the ring.
        """

        # Remove the node from the list of nodes
        self.nodes.remove(node)

        del self.hash_ring[self.get_hash(node.name)]


    def get_node(self, key):
        """
        Return the node in the ring that is responsible for the given key.
        """
        # Get the hash value of the key
        key_hash = self.get_hash(key) % max(self.hash_ring.keys())
         
        # Iterate over the keys in the hash ring until the key hash is less than or equal to a node hash
        for node_hash in sorted(self.hash_ring.keys()):
            if key_hash <= node_hash:
                return self.hash_ring[node_hash]

        # If the key hash is greater than all node hashes, wrap around to the first node
        return self.hash_ring[min(self.hash_ring.keys())]

    def get_hash(self, key):
        """
        Hash the given key using the murmur3_32 algorithm.
        """
        return murmur3_32(str(key))

# Initialize the Consistent Hash Ring
ring = ConsistentHashRing([])


def add_node(name, ip, port):

    # check if connection is possible
    try:
        client = Client((ip, port))
        client.set('name', name)
    except ConnectionRefusedError:
        print(f"\nConnection refused for {ip}:{port}")
        return

    except Exception as e:
        print(f"\nError: {e}")
        print("Please enter a valid IP address and port number")
        return

    # check if the node already exists
    for node in ring.nodes:
        
        if node.name == name:
            print("Node already exists")
            return
        if node.port == port:
            print("Port already in use")
            return
        
    new_node = Node(name, ip, port)
    ring.add(new_node)

    # get the next node in the ring
    next_node_index = (ring.nodes.index(new_node) + 1) % len(ring.nodes)
    next_node = ring.nodes[next_node_index]

    # redistribute the data from the next node to the new node
    for key, value in next_node.data.items():
        dht_set(key, value)

    # remove the redistributed data from the next node
    for key in next_node.data.keys():
        if dht_get(key) in [None, next_node.name]:
            del next_node.data[key]

    # remove the redistributed data from the original node
    for key in next_node.data.keys():
        if dht_get(key) == new_node.name:
            del next_node.data[key]
         
def remove_node(name):
    for node in ring.nodes:
        if node.name == name:
            # redistribute data to the next node
            for key in node.data.keys():
                if ring.get_node(key).name == node.name:
                    value = node.data[key]
                    dht_set(key, value)

            # remove node from ring
            ring.remove(node)
            return name
    print(f"\nNode {name} not found")

def dht_get(key):
    
    # check if a node exists in the ring
    if len(ring.nodes) == 0:
        print("No nodes in the ring")
        return
    # get the value for the key 
    node = ring.get_node(key)
    client = node.client
    value = client.get(key)
    
    # if the value is not None, return the value
    if value:
        return value.decode()
    else:
        next_node_index = (ring.nodes.index(node) + 1) % len(ring.nodes)
        next_node = ring.nodes[next_node_index]
        value = next_node.client.get(key)
        if value:
            return value.decode()

def dht_set(key, value):
    # check if a node exists in the ring
    if len(ring.nodes) == 0:
        print("No nodes in the ring")
        return
    # replication factor is 2
    nodes = [ring.get_node(key)]

    # get the next node
    next_node_index = (ring.nodes.index(nodes[0]) + 1) % len(ring.nodes)
    next_node = ring.nodes[next_node_index]
    nodes.append(next_node)

    for node in nodes:
        node.data[key] = value
        node.client.set(key, value)
        
# Helper function
def read_list_func(rlist):
    # Read the values of the sampled keys from your DHT
    print()
    print( "-"*50 )
    print()
    for key in rlist:
        value = dht_get(str(key))
        print(f"Key: {key}, Value: {value}")
    

def help_func():
    print("  Available commands:")
    print("  put - put a key-value pair into the DHT")
    print("  get - get the value of a key from the DHT")
    print("  add - add a new node to the DHT")
    print("  remove - remove a node from the DHT")
    print("  exit - exit the interactive shell")
    print("  help - display available commands")


def main():
    print("My Memcached DHT.")
	# Add Memcached Instances
    print("Initializing the DHT Cluster:")
    add_node('m1','localhost', 11211)
    add_node('m2','localhost', 11212)
    add_node('m3','localhost', 11213)
    add_node('m4','localhost', 11214)

	# # Write 100 key-value pairs to Memcached  
    print("Loading some fake data")
    fake = faker.Faker()
    for key in range(100):    
        value = fake.name()
        # key_hash = murmur3_32(str(key))
        print(f"Key: {key}, Value: {value} ")
        dht_set(str(key), value)

	### TEST
	# Sample 10 random keys to read from Memcached to test the system
    read_list = random.sample(range(100), 10)
    print("My Memcache DHT")

	# Check the status of the value
    read_list_func(read_list) # Check the content of the cache

    print("\nTesting the DHT")
	# Simulating the failure of a node m1
    remove_node('m1')
    read_list_func(read_list) # Check the content of the cache

    # remove_node('m1')
    # read_list_func(read_list) # Check the content of the cache

	# Simulating the addition of a new node m5
    add_node('m5','localhost', 11215)
    read_list_func(read_list) # Check the content of the cache

    # add_node('m6','localhost', 11216)
    # read_list_func(read_list) # Check the content of the cache

    # Interactive shell
    print("\n\n Interactive Shell:")
    while True:
        print("Enter command (put/get/add/remove/exit/help): ")
        command = input().strip().lower()
        
        if command == "put":
            key = input("Enter key: ")
            value = input("Enter value: ")
            if dht_set(key, value):
                print(f"Key: {key}, Value: {value} has been set")
        
        elif command == "get":
            key = input("Enter key: ")
            value = dht_get(key)
            if value is None:
                print(f"Key: {key} does not exist in the DHT")
            else:
                print(f"Key: {key}, Value: {value}")
        
        elif command == "exit":
            print("Exiting the DHT cluster.")
            break
        
        elif command == "help":
            help_func()

        elif command == "add":
            name = input("Enter name: ")
            ip = input("Enter IP address: ")
            try: 
                port = int(input("Enter port number: "))
            except ValueError:
                print("Port number must be an integer.")
                continue
            if add_node(name, ip, port):
                print(f"Node {name} has been added to the DHT")
        
        elif command == "remove":
            name = input("Enter name: ")
            if remove_node(name):
                print(f"Node {name} has been removed from the DHT")
        
        else:
            print(f"Invalid command: {command}. Type 'help' for available commands.")

if __name__ == "__main__":
    main()