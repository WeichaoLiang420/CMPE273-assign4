import hashlib

from server_config import NODES

class NodeRing():

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes
    
    def get_node(self, key_hex):
        key = int(key_hex, 16)
        node_index = key % len(self.nodes)
        return self.nodes[node_index]
    #redev
    def rv_hash(self,clientid, servers):
        hash = hashlib.md5()
        srv_dict = {}
        for srv in servers:
            srv_hash = srv.host+":"+str(srv.port) + clientid + '_%d'
            hash.update(srv_hash.encode('utf-8'))
            srv_dict[hash.hexdigest()] = srv
        client_goes_to = max(srv_dict.keys())
        print(clientid, " : ", srv_dict[client_goes_to])
        return srv_dict[client_goes_to]

    def get_rv_node(self,key_hex):
        key = str(key_hex)
        print(key)
        return self.rv_hash(key,self.nodes)

    #consistent hash
    def search(self,circle, hash_client):
        keys = sorted(circle.keys())
        for i, j in enumerate(keys):
            if keys[i - 1] < hash_client <= keys[i]:
                return circle[keys[i]]
        return circle[keys[0]]

    def cons_hash(self,clientid, servers):
        number_of_node = 10000
        hash = hashlib.md5()
        circle = {}
        for srv in servers:
            for i in range(number_of_node):
                srv_hash = srv.host+":"+str(srv.port) + '_%d' % i
                hash.update(srv_hash.encode('utf-8'))
                circle[hash.hexdigest()] = srv
        hash.update(clientid.encode('utf-8'))
        #print(clientid, " : ", self.search(circle, hash.hexdigest()))
        return self.search(circle, hash.hexdigest())

    def get_consis_node(self,key_hex):
        key = str(key_hex)
        print(key)
        return self.cons_hash(key,self.nodes)


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
#test()
