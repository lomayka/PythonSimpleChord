from client import Client
from threading import Thread
from twisted.internet import reactor
from time import sleep
import socket

import sys

def send_shortcut(destination_id, starting_id, endpoint_id):
    ip = "127.0.0.1"
    port = 10000+destination_id
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
    string = "new_shortcut;" + str(starting_id) + ";" + str(endpoint_id)
    s.sendto(string.encode('utf-8'), (ip, port))

def send_connect(destination_id, new_id):
    reactor.listenUDP(10000+new_id, Client(new_id, 'localhost', starting_id=destination_id))
    Thread(target=reactor.run, args=(False,)).start()

def send_leave(destination_id, leave_id):
    ip = "127.0.0.1"
    port = 10000+destination_id
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
    string = "new_leave;" + str(leave_id) + ";" + str(destination_id)
    s.sendto(string.encode('utf-8'), (ip, port))

def send_lookup(destination_id, search_key):
    ip = "127.0.0.1"
    port = 10000+destination_id
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
    string = "lookup;" + str(search_key) + ";" + str(0)
    s.sendto(string.encode('utf-8'), (ip, port))

def send_list(destination_id):
    ip = "127.0.0.1"
    port = 10000+destination_id
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
    s.sendto("list;".encode('utf-8'), (ip, port))

def check_range(node, key_space):
    if int(node)<int(key_space[0]) or int(node)>int(key_space[1]):
        return False
    else:
        return True

def read_file(filename):
    with open(filename, mode='rt', encoding='utf-8') as f:
        read_data = f.read().splitlines()
        for line in range(len(read_data)):
            if read_data[line] == '#key-space':
                key_space = read_data[line+1].split(',')
            elif read_data[line] == '#nodes':
                nodes = list(map(int, read_data[line+1].split(', ')))
            elif read_data[line] == '#shortcuts':
                shortcuts = read_data[line+1].split(',')
    return key_space, nodes, shortcuts

def command_input(command, nodes, key_space):
    nodes=sorted(nodes)
    command = command.split(' ')
    if command[0] == 'List':
        send_list(int(nodes[0]))
        
    
    elif command[0] == 'Lookup':
        lookup_id = command[1].split(':')
        if len(lookup_id) == 2:
            send_lookup(int(lookup_id[1]),int(lookup_id[0]))
        else:
            send_lookup(int(nodes[0]),int(lookup_id[0]))
        
        
    elif command[0] == 'Join':
        join_id = command[1]
        if check_range(join_id, key_space) == False:
            print("This node is not valid!")
        else:
            nodes.append(int(join_id))
            send_connect(int(nodes[0]),int(join_id))
      
    elif command[0] == 'Leave':
        leave_id = command[1]
        send_leave(int(nodes[0]),leave_id)
        nodes.remove(int(leave_id))

        
    elif command[0] == 'Shortcut':
        shortcut_id = command[1].split(':')
        if check_range(shortcut_id[0], key_space) and check_range(shortcut_id[1], key_space):
            starting_id = shortcut_id[0]
            endpoint_id = shortcut_id[1]
            send_shortcut(nodes[0],starting_id, endpoint_id)
        else:
            print("shortcuts out of range")
    else:
        print("You have a wrong input!")

    return nodes

key_space, nodes, shortcuts = read_file("input-file.txt")
print(key_space, nodes, shortcuts)
for id in nodes:
    sleep(0.01)
    if check_range(id, key_space):
        reactor.listenUDP(10000+int(id), Client(int(id), 'localhost', int(nodes[0])))
        Thread(target=reactor.run, args=(False,)).start()
    else:
        nodes.remove(id)
        print("This node is not valid!")

for s in shortcuts:
    s = s.split(':')
    send_shortcut(int(nodes[0]), int(s[0]), int(s[1]))
# reactor.listenUDP(10000, Client(0, 'localhost', starting_id=-1))
# Thread(target=reactor.run, args=(False,)).start()

command = input()
while command != 'end process':
    nodes = command_input(command, nodes, key_space)
    command = input()
reactor.stop()
sys.exit()
