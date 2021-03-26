from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys
from time import sleep

class Client(DatagramProtocol):
    def __init__(self, id, host, starting_id=-1):
        if host == "localhost":
            self.host = "127.0.0.1"
        
        self.id = id
        self.successor_id = id
        self.next_successor_id = id
        self.addr = None
        self.starting_id = starting_id
        self.successor_increment = 0
        self.shortcuts = {}
    
    def datagramReceived(self, datagram, addr):
        datagram = datagram.decode("utf-8")
        self.process_datagram(datagram)
        
    def startProtocol(self):
        if self.starting_id != -1:
            self.join_request(self.starting_id)
        reactor.callInThread(self.successor_checker)

    def process_datagram(self, string):
        splits = string.split(";")
        function = splits[0]
        if function == "find":
            self.find_successor(int(splits[1]), int(splits[2]), int(splits[3]))
        elif function == "found":
            self.successor_id = int(splits[1])         
        elif function == "collect":
            self.send_collect(int(splits[1]), splits[2])
        elif function == "new_next":
            self.next_successor_id = int(splits[1])
        elif function == "new_leave":
            self.leave(int(splits[1]), int(splits[2]), starting=True)
        elif function == "leave":
            self.leave(int(splits[1]), int(splits[2]))
        elif function == "check":
            self.send_check_response(int(splits[1]))
        elif function == "up":
            self.next_successor_id = int(splits[2])
            self.successor_increment = 0
        elif function == "next":
            self.send_new_next(int(splits[1]))
        elif function == "list":
            self.send_collect_request(self.id, "")
        elif function == "shortcut":
            self.process_shortcut(int(splits[1]), int(splits[2]), int(splits[3]))
        elif function == "new_shortcut":
            self.process_shortcut(int(splits[1]), int(splits[2]), self.id, starting=True)
        elif function == "lookup":
            self.lookup(int(splits[1]), int(splits[2]))
        elif function == "shortcut_check":
            self.send_shortcut_response(int(splits[1]))
        elif function == "shortcut_responce":
            self.shortcuts[int(splits[1])] = 0

    def join_request(self, join_id):
        self.find_next_successor_id(self.id, self.id, join_id=join_id)

    def find_successor(self, id, source_id, prev_id):
        if (self.id < id and self.successor_id > id) or self.id == self.successor_id or (self.id > self.successor_id and self.id < id) or (self.id > self.successor_id and self.id > id):
            self.send_successor_id_response(source_id, prev_id)
            self.successor_id = id
        else:
            self.find_next_successor_id(id, source_id)

    def send_collect(self, source_id, res):
        if(self.id == source_id):
            print(res)
        else:
            self.send_collect_request(source_id, res)

    def leave(self, target_id, source_id, starting=False):
        if target_id == self.id:
            self.transport.stopListening()
        elif source_id == self.id and not starting:
            print("no node with chosen id")
        else:
            self.send_leave(target_id, source_id)

    def process_shortcut(self, target_id, shortcut_id, source_id, starting=False):
        if(target_id == self.id):
            self.shortcuts[shortcut_id] = 0
        elif source_id == self.id and not starting:
            print("there is no such node")
        else:
            self.forward_shortcut(target_id, shortcut_id, source_id)

    def lookup(self, search_key, acc):
        if search_key < self.id:
            print("Data stored in node", self.id, " - ", acc, "requests sent")
        elif search_key < self.successor_id or (self.successor_id < self.id):
            print("Data stored in node", self.successor_id, " - ", acc+1, "requests sent")
        else:
            sent = False
            for shor in sorted(self.shortcuts, reverse=True):
                if (shor < search_key):
                    sent = True
                    self.send_lookup(shor, search_key, acc+1)
                    break
            if not sent:
                self.send_lookup(self.successor_id, search_key, acc+1)

    def successor_checker(self):
        while True:
            
            sleep(0.2)
            self.successor_increment += 1
            if self.successor_increment % 5 == 4:
                self.send_successor_check()
            if self.successor_increment % 21 == 20:
                self.successor_id = self.next_successor_id
                self.get_next_successor_id()
            for k in list(self.shortcuts):
                self.shortcuts[k] += 1
                if self.shortcuts[k] % 5 == 4:
                    self.send_shortcut_check(k)
                if self.shortcuts[k] % 21 == 20:
                    del self.shortcuts[k]


    def send_successor_check(self):
        string = "check;" + str(self.id)
        addr = self.host, 10000+self.successor_id
        self.transport.write(string.encode('utf-8'), addr)

    def send_check_response(self, source_id):
        string = "up;" + str(self.id) + ";" + str(self.successor_id)
        addr = self.host, 10000+source_id
        self.transport.write(string.encode('utf-8'), addr)

    def get_next_successor_id(self):
        string = "next;" + str(self.id)
        addr = self.host, 10000+self.successor_id
        self.transport.write(string.encode('utf-8'), addr)
    
    def send_new_next(self, source_id):
        string = "new_next;" + str(self.successor_id)
        addr = self.host, 10000+source_id
        self.transport.write(string.encode('utf-8'), addr)

    def forward_shortcut(self, target_id, shortcut_id, source_id):
        string = "shortcut;" + str(target_id) + ";" + str(shortcut_id) + ";" + str(source_id)
        addr = self.host, 10000+self.successor_id
        self.transport.write(string.encode('utf-8'), addr)

    def send_lookup(self, target_id, search_key, acc):
        string = "lookup;" + str(search_key) + ";" + str(acc)
        addr = self.host, 10000+target_id
        self.transport.write(string.encode('utf-8'), addr)
    
    def send_shortcut_check(self, target_id):
        string = "shortcut_check;" + str(self.id)
        addr = self.host, 10000+target_id
        self.transport.write(string.encode('utf-8'), addr)
    
    def send_shortcut_response(self, source_id):
        string = "shortcut_responce;" + str(self.id)
        addr = self.host, 10000+source_id
        self.transport.write(string.encode('utf-8'), addr)

    def send_leave(self, target_id, source_id):
        string = "leave;" + str(target_id) + ";" + str(source_id)
        addr = self.host, 10000+self.successor_id
        self.transport.write(string.encode('utf-8'), addr)

    def send_collect_request(self, source_id, res):
        shorts = ""
        for s in self.shortcuts:
            shorts += str(s) + ","
        res += str(self.id) + ":" +  shorts + "S-" +  str(self.successor_id) + ",NS-" + str(self.next_successor_id) + "\n"
        string = "collect;" + str(source_id) + ";" + res
        addr = self.host, 10000+self.successor_id
        self.transport.write(string.encode('utf-8'), addr)

    def send_successor_id_response(self, source_id, prev_id):
        string = "found;" + str(self.successor_id) + ";" + str(self.next_successor_id)
        addr = self.host, 10000+source_id
        self.transport.write(string.encode('utf-8'), addr)
        string = "new_next;" + str(source_id)
        addr = self.host, 10000+prev_id
        self.transport.write(string.encode('utf-8'), addr)

    def find_next_successor_id(self, id, source_id, join_id=-1):
        string = "find;" + str(id) + ";" + str(source_id) + ";" + str(self.id)
        if join_id == -1:
            send_request_id = self.successor_id
        else:
            send_request_id = join_id
        addr = self.host, 10000+send_request_id
        self.transport.write(string.encode('utf-8'), addr)
