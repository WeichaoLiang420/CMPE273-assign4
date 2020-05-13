import sys
import socket

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_DELETE, serialize_PUT,serialize_GET
from node_ring import NodeRing
#from bloom_filter import BloomFilter
#from RHW_hashing import rendezvous_hashing
BUFFER_SIZE = 1024
import hashlib
class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)       

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):
    print("parameter: ",udp_clients)


    client_ring = NodeRing(udp_clients) # ???????
    #hrw = rendezvous_hashing(client_ring, [100,200,300,400])

    #hrw= RHW_hashing.Ring()




    print("client_ring",client_ring)
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        response = client_ring.get_node(key).send(data_bytes)
        # response = hrw.get_responsible_node(key).send(data_bytes)
        #lcu.cash add(response)
        hash_codes.add(str(response.decode()))
        # print("users",u)
        # print("hashcode:",hash_codes)
        print("user numbers:"f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")
    
    # GET all users.
    for hc in hash_codes:
        print("hashcode:",hc)
        data_bytes, key = serialize_GET(hc)
        print("data_bytes: ",data_bytes)
        print("key",key)
        #cash 
        #bloomingfilter
        response = client_ring.get_node(key).send(data_bytes)
        print("response:",response)
    #delete
    for hc in hash_codes:
        data_bytes,key = serialize_DELETE(hc)
        response = client_ring.get_node(key).send(data_bytes)
        print("success")

def rv_hash(clientid, servers):
	hash = hashlib.md5()
	srv_dict = {}
	for srv in servers:
		srv_hash = srv + clientid + '_%d'
		hash.update(srv_hash.encode('utf-8'))
		srv_dict[hash.hexdigest()] = srv
	client_goes_to = max(srv_dict.keys())
	print(clientid, " : ", srv_dict[client_goes_to])

# def process_rv_hash(udp_clients):
#     print("parameter: ", udp_clients)
#     client_ring = NodeRing(udp_clients)
#     servers=["s1","s2", "s3", "s4"]
 
#     rv_hash("client-01",["s1", "s2", "s3", "s4"])
#     rv_hash("client-02", ["s1", "s2", "s3", "s4"])
#     rv_hash("client-03", ["s1", "s2", "s3", "s4"])
#     rv_hash("client-04", ["s1", "s2", "s3", "s4"])

def process_rv_hash(udp_clients):
    print("parameter: ", udp_clients)
    hash_codes = set()
    client_ring = NodeRing(udp_clients)
    for u in USERS:
        data_bytes, key = serialize_PUT(u)

        #response = client_ring.get_node(key).send(data_bytes)
        print("####")
        print(key)
        #hash_codes.add(str(response.decode()))
        #print("Users number="+ str(hash_codes))
        #print("Users"+str(USERS))
        print("User"+str(u)) #each user
        #print(response.decode()) # each user hash value
        #print("user numbers:"f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")
        rv_hash(key,["127.0.0.1:4000", "127.0.0.1:4001", "127.0.0.1:4002", "127.0.0.1:4003"])
        response=client_ring.get_rv_node(key).send(data_bytes)



def search(circle, hash_client):
    keys = sorted(circle.keys())
    for i, j in enumerate(keys):
        if keys[i-1] < hash_client <= keys[i]:
            return circle[keys[i]]
    return circle[keys[0]]


def cons_hash(clientid, servers):
	number_of_node = 10000
	hash = hashlib.md5()
	circle = {}
	for srv in servers:
		for i in range(number_of_node):
			srv_hash = srv + '_%d' % i
			hash.update(srv_hash.encode('utf-8'))
			circle[hash.hexdigest()] = srv
	hash.update(clientid.encode('utf-8'))
	print(clientid, " : ", search(circle, hash.hexdigest()))

def process_consis_hash(udp_clients):
    print("parameter: ", udp_clients)
    hash_codes = set()
    client_ring = NodeRing(udp_clients)
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        print("####")
        print(key)
        print("User" + str(u))  # users
        cons_hash(key, ["127.0.0.1:4000", "127.0.0.1:4001", "127.0.0.1:4002", "127.0.0.1:4003"])
        response = client_ring.get_consis_node(key).send(data_bytes)

def process_cons_hash(udp_clients):
    cons_hash('client-06', ["s1", "s2", "s3", "s4", "s5", "s6"])
    print("parameter: ", udp_clients)
    client_ring = NodeRing(udp_clients)
    servers = ["s1", "s2", "s3", "s4"]
    cons_hash("client-01", ["s1", "s2", "s3", "s4"])
    cons_hash("client-02", ["s1", "s2", "s3", "s4"])
    cons_hash("client-03", ["s1", "s2", "s3", "s4"])
    cons_hash("client-04", ["s1", "s2", "s3", "s4"])




if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])#???进入的是第一个 def？？？？？
        for server in NODES   
    ]
    #consisprocess(clients)
   # process(clients)
    process_rv_hash(clients)
    #process_cons_hash(clients)
    process_consis_hash(clients)
