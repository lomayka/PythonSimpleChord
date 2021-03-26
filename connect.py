from client import Client
from twisted.internet import reactor
import sys

def send_shortcut(destination_id, starting_id, endpoint_id):
    ip = "127.0.0.1"
    port = 10000+destination_id
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
    string = "new_shortcut;" + str(starting_id) + ";" + str(endpoint_id)
    s.sendto(string.encode('utf-8'), (ip, port))

def send_connect(destination_id, new_id):
    reactor.listenUDP(10000+new_id, Client(new_id, 'localhost', destination_id))
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

def command_input(command, nodes, key_space):
    nodes=sorted(nodes)
    command = command.split(' ')
    if command[0] == 'List':
        send_list(sys.args[0])
        
    
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
            send_connect(int(nodes[0]),int(join_id))
      
    elif command[0] == 'Leave':
        leave_id = command[1]
        send_leave(int(nodes[0]),leave_id)
        print(nodes)
        nodes.remove(int(leave_id))
        
    elif command[0] == 'Shortcut':
        if check_range(shortcut_id[0], key_space) and check_range(shortcut_id[1], key_space):
            shorcut_id = command[1].split(':')
            starting_id = shortcut_id[0]
            endpoint_id = shortcut_id[1]
            send_shortcut(nodes[0],starting_id, endpoint_id)
        else:
            print("shortcuts out of range")
    else:
        print("You have a wrong input!")


id = int(sys.argv[1])
starting_id = int(sys.argv[2])
reactor.listenUDP(10000+id, Client(id, 'localhost', starting_id))
reactor.run()

command = input()
while command != 'end process':
    command_input(command, nodes, "0,100")
    command = input()
reactor.stop()
sys.exit()