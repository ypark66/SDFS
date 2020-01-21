import socket
import sys
import json
import os
import re
import glob
import string
import random
import threading
from logger import logger
from failure_detector import RingFailureDetector

class Service:
    def __init__(self, log_directory = 'log/service.log'):
        self.logging = logger('service',log_directory).logger
        self.fd = RingFailureDetector()
        self.halt = 0

    def gen_random_text(self,length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))


    def create_test_file(self,file_name, good_text, random_count, good_count):
        line_list = []
        for a in range(random_count):#adding random lines
            line_list.append(self.gen_random_text(len(good_text)))
        for a in range(good_count):#adding good lines
            line_list.insert(random.randint(1, len(line_list)-1), good_text)

        with open(file_name, 'w') as f:
            f.writelines(line_list)


    def handle_gen_files(self, packet):
        self.create_test_file(packet['file_name'], packet['line_text'], packet['random_count'], packet['good_count'])
        return {'type': 'make_test_return',
                'data': 'ok'}


    def handle_grep(self, packet,f_location = "*.log"):
        self.logging.info('grep argument: {0}'.format(packet['args'][0]))

        #for mp1, ret returns the number of counts for given regex.
        #query lines are stored in the variable 'log'
        ret = ''
        log = ""
        for file in glob.glob(f_location):
            self.logging.info('searching file {0}'.format(file))
            if ret != '':
                ret+='\n'
                log+='\n'
            ret += str(file) + ': '
            count = 0
            for line in open(file, 'r'):
                if re.search(packet['args'][0], line):
                    log +=line
                    count += 1
            ret += str(count)

        self.logging.info('Matching Number of lines: {0}'.format(ret))
        if f_location != "*.log":
            ret = log
        return {'type': 'grep_return',
                'data': ret}



    def handle_request(self, packet):
        '''
        Takes the passed json packet as a string and returns the correct response packet
        after doing the necesary actions in string format
        '''
        resp_packet = {'type': 'grep_return',
                'data': None}  # check for default packet
        packet_data = json.loads(packet)
        if packet_data['type'] == 'grep':
            self.logging.info('Handling Grep Request')
            resp_packet = self.handle_grep(packet_data)
        elif packet_data['type'] == 'gen_test_files':
            resp_packet = self.handle_gen_files(packet_data)
        elif packet_data['type'] == 'log':
            self.logging.info('Handling log query request')
            resp_packet = self.handle_grep(packet_data, 'log/*.log')
        elif packet_data['type'] == 'halt':
            self.logging.info('Handling halt request')
            return 0
        else:
            self.logging.warning('Wrong argument passed in')
        
        response = json.dumps(resp_packet)
        return response


    def listen_loop(self):
        '''
        Listen for connections on a loop and handle the requests
        as they come in and return the correct responses
        '''

        failure_detection_thread = threading.Thread(target=self.fd.failure_detection)
        self.logging.info('Service initiated.')
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            
            failure_detection_thread.start()

            server_address = ('0.0.0.0', 2156)
            s.bind(server_address)
            s.listen()

            while self.halt == 0:
                self.logging.info('waiting for a connection')
                connection, client_address = s.accept()
                
                with connection:
                    self.logging.info('Connected by ' + str(client_address))

                    while self.halt == 0:
                        try:
                            data = connection.recv(1048576)
                        except ConnectionResetError:
                            self.logging.error('Client connection error')
                            break

                        self.logging.info('Data received from client: {0}'.format(data))
                        if not data:
                            self.logging.info('Closing connection')
                            break

                        message = self.handle_request(data.decode())  #handle request
                        if message == 0:
                            message = json.dumps({'type': 'halt_return', 'data': True})
                            connection.sendall(message.encode())
                            self.halt = 1
                            break
                        self.logging.info("Sending message back to client")

                        try:
                            connection.sendall(message.encode())    #sending data back
                        except ConnectionResetError:
                            self.logging.error('Connection reset by peer')
                            break
                        
            self.logging.info('Halting service')




#TODO: Add a thread to send heartbeat every given amount of time / Use UDP
#TODO: Add a thread to check for failure
#TODO: Add joining a node
#TODO: Add deleting a node
#TODO: Sending detected failure to all nodes in the system within 6 seconds
#TODO: Implement functions for introducer

if __name__ == "__main__":
    s = Service()
    s.listen_loop()
    print('terminating server')
    s.logging.info('terminating server')


