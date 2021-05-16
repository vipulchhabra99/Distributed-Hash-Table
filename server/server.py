import socket
import threading
import time
import hashlib
import random
import sys
from copy import deepcopy
from database import Database
from fingertable import FingerTable
import argparse
from flask import Flask, request
import requests
from hash import Hasher
app = Flask(__name__)
m = 7
hash_fun = Hasher()

class Node:
    
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)
        self.nodeinfo = self.ip + "|" + str(self.port)
        self.id = hash_fun.hash(str(self.nodeinfo))
        self.finger_table = FingerTable(self.id)
        self.data_store = Database()
        self.operations = ['insert_current', 'update_current', 'delete_current', 'search_current']
        self.ring_operations = ['send_keys', 'insert_in_other_node', 'delete_in_other_node', 'search_in_other_node',
                                'update_in_other_node', 'join_request', 'find_predecessor', 'find_successor',
                                'get_successor', 'get_predecessor', 'get_id', 'notify']
    

    def ring_creation(self):
        self.predecessor = self
        self.successor = self
        self.finger_table.set_successor(self, 0)

    def operate_cur_node(self, operation, args):

        result = "Reqeust Not Processed!"

        if operation == 'insert_current':
            data = args[0].split("=") 
            key = data[0].strip()
            value = data[1].strip()
            if(self.data_store.isPresent(key) == False):
                self.data_store.insert(key, value)
                result = 'Key Inserted!'
            else:
                result = 'Key Already Present!'

        if operation == "delete_current":

            key = args[0].strip()
            output = self.data_store.delete(key)

            if(output == True):
                result = 'Deleted!'

            else:
                result = 'Key not found!'

        if operation == "search_current":

            key = args[0].strip()
            result = self.data_store.search(key)
            if(result != None):
                return result
            else:
                return "Key Not Present!"

        if operation == "update_current":

            data = args[0].split('=')
            key = data[0].strip()
            value = data[1].strip()
            if(self.data_store.isPresent(key) == True):
                self.data_store.update(key, value)
                result = 'Key Updated Successfully!'

            else:
                result = 'Key Not present!'

        return result

    def insert_key(self, key, value):

        id_of_key = hash_fun.hash(str(key))
        succ = self.find_successor(id_of_key)
        ip, port = self.get_ip_port(succ)

        link = "http://" +ip+":"+str(port)+"/insert_current"

        if(ip == self.ip and port == self.port):
                self.operate_cur_node("insert_current", [str(key) + " = " + str(value)])

        else:
            link += "?key="+str(key)+"&value=" + str(value)
            requests.get(link)
        
        return("Inserted at node id " + str(Node(ip, port).id))

    def delete_key(self, key):
        id_of_key = hash_fun.hash(str(key))
        succ = self.find_successor(id_of_key)
        ip, port = self.get_ip_port(succ)


        link = "http://" +ip+":"+str(port)+"/delete_current"

        if(ip == self.ip and port == self.port):
                self.operate_cur_node("delete_current", [str(key)])

        else:
            link += "?key="+str(key)
            requests.get(link)
        
        return("Deleted at node id " + str(Node(ip, port).id))

    def update_key(self, key, value):
        id_of_key = hash_fun.hash(str(key))
        succ = self.find_successor(id_of_key)
        ip, port = self.get_ip_port(succ)


        link = "http://" +ip+":"+str(port)+"/update_current"

        if(ip == self.ip and port == self.port):
                self.operate_cur_node("update_current", [str(key) + " = " + str(value)])

        else:
            link += "?key="+str(key)+"&value=" + str(value)
            requests.get(link)

        return("Updated at node id " + str(Node(ip, port).id))

    def search_key(self, key):
        id_of_key = hash_fun.hash(str(key))
        succ = self.find_successor(id_of_key)
        ip, port = self.get_ip_port(succ)

        message = "search_current : " + str(key)

        link = "http://" +ip+":"+str(port)+"/search_current"

        if(ip == self.ip and port == self.port):
            return self.operate_cur_node("search_current", [str(key)])

        else:
            link += "?key="+str(key)
            return requests.get(link).content.decode("utf-8")


    def join(self, node_ip, node_port):

        link = "http://" +node_ip+":"+str(node_port)+"/join_request"
        succ = None
        if(node_ip == self.ip and node_port == self.port):
            succ = self.find_successor(int(self.id))

        else:
            link += "?id="+str(self.id)
            succ = requests.get(link).content.decode("utf-8")

        ip, port = self.get_ip_port(succ)
        successor_node = Node(ip, port)
        self.successor = successor_node
        self.finger_table.set_successor(successor_node, 0)
        self.predecessor = None

        if successor_node.id != self.id:
            
            link = "http://" +ip+":"+str(port)+"/send_keys"
            data = None
            if(ip == self.ip and port == self.port):
                data = self.find_successor(int(self.id))

            else:
                link += "?id="+str(self.id)
                data = requests.get(link).content.decode("utf-8")
            
            for key_value in data.split(','):
                if len(key_value) > 1:

                    key, value = key_value.split('=')
                    key = key.strip()
                    value = value.strip()
                    self.data_store.insert(key, value)

    def find_predecessor(self, search_id):

        if search_id == self.id:
            return self.nodeinfo

        if self.predecessor is not None and self.successor.id == self.id:
            return self.nodeinfo
            
        if self.get_forward_distance(
                self.successor.id) > self.get_forward_distance(search_id):
            return self.nodeinfo
        else:
            new_node_hop = self.closest_preceding_node(search_id)
            if new_node_hop is not None:
                ip, port = self.get_ip_port(new_node_hop.nodeinfo)
                if ip == self.ip and port == self.port:
                    return self.nodeinfo
                
                link = "http://" +ip+":"+str(port)+"/find_predecessor"
                data = None
                if(ip == self.ip and port == self.port):
                    data = self.find_predecessor(int(search_id))


                else:
                    link += "?id="+str(search_id)
                    data = requests.get(link).content.decode("utf-8")
                
                return data
            else:
                return str(None)

    def find_successor(self, search_id):
        
        if (search_id == self.id):
            return self.nodeinfo
        predecessor = self.find_predecessor(search_id)
        if (str(predecessor) == "None"):
            return str(predecessor)
        ip, port = self.get_ip_port(predecessor)
        if(self.ip == ip and self.port == port):
            return self.get_successor()
        else:
            link = "http://" + ip+":"+str(port)+"/get_successor"
            data = requests.get(link).content.decode("utf-8")
            return data

    def closest_preceding_node(self, search_id):
        min_distance = (2**m) + 1
        for i in range(m-1, -1, -1):
            if(self.finger_table.get_successor(i) != None and 
                self.get_forward_distance_2nodes(self.finger_table.get_successor(i).id,
                        search_id) < min_distance):

                closest_node = self.finger_table.get_successor(i)
                min_distance = self.get_forward_distance_2nodes(
                    self.finger_table.get_successor(i).id, search_id)

        try:
            closest_node
        except:
            closest_node = None

        return closest_node

    def send_keys(self, id_of_joining_node):

        data = ""
        keys_to_be_removed = []
        current_db = self.data_store.get_all()

        for keys in current_db:
            key_id = hash_fun.hash(str(keys))
            if self.get_forward_distance_2nodes(key_id, id_of_joining_node) < self.get_forward_distance_2nodes(key_id, self.id):
                keys_to_be_removed.append(keys)

        for keys in keys_to_be_removed:
            data += str(keys) + " = " + str(current_db[keys]) + " , "
            
        for keys in keys_to_be_removed:
            self.data_store.delete(keys)
        
        return data

    def stabilize(self):

        while True:
            if self.successor is not None:

                data = "get_predecessor"

                if self.nodeinfo.strip() == self.successor.nodeinfo.strip():
                    time.sleep(10)
                
                link = "http://" + self.successor.ip+":"+str(self.successor.port)+"/get_predecessor"

                if(self.successor.ip == self.ip and self.successor.port == self.port):
                    result = self.get_predecessor()

                else:
                    result = requests.get(link).content.decode("utf-8")
                
                if result == "None" or len(result) == 0:
                    link = "http://" + self.successor.ip+":"+str(self.successor.port)+"/notify"

                    if(self.successor.ip == self.ip and self.successor.port == self.port):
                        self.notify(self.id, self.ip, self.port)

                    else:
                        link += "?id="+str(self.id)+"&ip=" + str(self.ip) + "&port=" + str(self.port)
                        requests.get(link)
                    continue

                ip, port = self.get_ip_port(result)

                link = "http://" + ip+":"+str(port)+"/get_id"

                if(ip == self.ip and port == self.port):
                    result = self.get_id()

                else:
                    result = requests.get(link).content.decode("utf-8")
                result = int(result)

                if self.get_forward_distance(result) < self.get_forward_distance(self.successor.id):
                    self.successor = Node(ip, port)
                    self.finger_table.set_successor(self.successor, 0)
                
                link = "http://" + self.successor.ip+":"+str(self.successor.port)+"/notify"

                if(self.successor.ip == self.ip and self.successor.port == self.port):
                    self.notify(self.id, self.ip, self.port)

                else:
                    link += "?id="+str(self.id)+"&ip=" + str(self.ip) + "&port=" + str(self.port)
                    requests.get(link)
                print()
                print()
                print()
                print("===============================================")
                print("DATA STORE")
                print("===============================================")
                print(str(self.data_store.data))
                print("===============================================")
                print()
                print()
                print()
            time.sleep(10)

    def notify(self, node_id, node_ip, node_port):
        
        if str(self.predecessor) == "None":
            self.predecessor = Node(node_ip, int(node_port))
            self.successor = self.predecessor
            self.finger_table.set_successor(self.successor, 0)
        
        else:
            if(self.get_backward_distance(self.predecessor.id) > self.get_backward_distance(node_id)):
                self.predecessor = Node(node_ip, int(node_port))

            elif(node_id < self.id and self.predecessor.id < node_id):
                self.predecessor = Node(node_ip, int(node_port))
                self.successor = self.predecessor
                self.finger_table.set_successor(self.successor, 0)

            elif(self.predecessor.id == self.id and self.id != node_id):
                self.predecessor = Node(node_ip, int(node_port))
                self.successor = self.predecessor
                self.finger_table.set_successor(self.successor, 0)


    def fix_fingers(self):

        iter_num = 1
        while True:
            iter_num = (iter_num % (m-1)) + 1
            finger = self.finger_table.get_table_enteries(iter_num)
            data = self.find_successor(finger[0])
            if data != "None":
                ip, port = self.get_ip_port(data)
                self.finger_table.table[iter_num][1] = Node(ip, port)
                iter_num += 1
            time.sleep(15)

    def get_successor(self):

        if self.successor is None:
            return str(self.successor)
        else:
            return self.successor.nodeinfo

    def get_predecessor(self):

        if self.predecessor is None:
            return str(self.predecessor)

        else:
            return self.predecessor.nodeinfo

    def get_id(self):

        return str(self.id)

    def get_ip_port(self, string_format):
        ip, port = string_format.strip().split('|')
        ip = ip.strip()
        port = int(port.strip())
        return ip, port

    def get_backward_distance(self, node1):

        distance = self.id - node1
        if(distance < 0):
            distance = pow(2, m) + distance
        return distance

    def get_backward_distance_2nodes(self, node2, node1):

        distance = node2 - node1
        if (distance < 0):
            distance = pow(2, m) + distance
        return distance

    def get_forward_distance(self, nodeid):
        
        distance = self.id - nodeid
        if(distance < 0):
            return abs(distance)
        else:
            return pow(2, m) - distance

    def get_forward_distance_2nodes(self, node2, node1):
        
        distance = node2 - node1
        if (distance < 0):
            return abs(distance)
        else:
            return pow(2, m) - distance


parser = argparse.ArgumentParser(description='Create a Ring/ Add new node to the ring')
parser.add_argument("port", help="prt on which the current node will listen requests",
                type=int)
parser.add_argument('-r',"--ringport", help="Port of the node of the ring available to join",
                type=int)
args = parser.parse_args()

ip = "0.0.0.0"

if args.port and args.ringport:
    print("JOINING RING")
    node = Node(ip, args.port)
    node.predecessor = None
    node.successor = None
    node.join(ip,args.ringport)
    

elif args.port:
    print("CREATING RING")
    node = Node(ip, args.port)
    node.predecessor = None
    node.successor = None
    node.ring_creation()
    
    
else:
    print("Please pass the required arguments!")
    exit()

@app.route('/get_successor')
def get_successor():
    return node.get_successor()

@app.route('/get_predecessor')
def get_predecessor():
    return node.get_predecessor()

@app.route('/get_id')
def get_id():
    return node.get_id()

@app.route('/notify')
def notify():
    id = request.args.get("id")
    ip = request.args.get("ip")
    port = request.args.get("port")
    node.notify(int(id), ip, int(port))
    return "Notified"

@app.route('/insert_current')
def insert_current():
    key = request.args.get("key")
    value = request.args.get("value")
    return node.operate_cur_node("insert_current", [str(key) + " = " + str(value)])

@app.route('/delete_current')
def delete_current():
    key = request.args.get("key")
    return node.operate_cur_node("delete_current", [str(key)])

@app.route('/search_current')
def search_current():
    key = request.args.get("key")
    return node.operate_cur_node("search_current", [str(key)])

@app.route('/update_current')
def update_current():
    key = request.args.get("key")
    value = request.args.get("value")
    return node.operate_cur_node("update_current", [str(key) + " = " + str(value)])


@app.route('/join_request')
def join_request():
    id = request.args.get("id")
    return node.find_successor(int(id))

@app.route('/find_predecessor')
def find_predecessor():
    id = request.args.get("id")
    return node.find_predecessor(int(id))

@app.route('/send_keys')
def send_keys():
    id = request.args.get("id")
    id_of_joining_node = int(id)
    return node.send_keys(id_of_joining_node)

@app.route('/insert_in_other_node')
def insert_in_other_node():
    key = request.args.get("key")
    value = request.args.get("value")
    key = key.strip()
    value = value.strip()
    return node.insert_key(key, value)

@app.route('/delete_in_other_node')
def delete_in_other_node():
    key = request.args.get("key")
    key = key.strip()
    return node.delete_key(key)

@app.route('/update_in_other_node')
def update_in_other_node():
    key = request.args.get("key")
    value = request.args.get("value")
    key = key.strip()
    value = value.strip()
    return node.update_key(key, value)

@app.route('/search_in_other_node')
def search_in_other_node():
    key = request.args.get("key")
    key = key.strip()
    return node.search_key(key)

@app.route('/find_successor')
def find_successor():
    id = request.args.get("id")
    return node.find_successor(int(id))


thread_for_stabalize = threading.Thread(target=node.stabilize)
thread_for_fix_finger = threading.Thread(target=node.fix_fingers)
thread_for_app = threading.Thread(target=app.run, args=(ip, args.port))
thread_for_stabalize.start()
thread_for_fix_finger.start()
thread_for_app.start()