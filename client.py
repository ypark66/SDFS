import socket
import sys
import threading
import time
import random
import argparse
import json
import os
import tqdm

PORT = 1234


def fetch_data(ip, command):
    '''
    Connect to the server
    send the request packet
    wait for the respones
    close the connection
    return the response
    '''
    # logging = logger(ip,'log/{0}.log'.format(ip)).logger
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_address = (ip, PORT)

    try:
        sock.connect(server_address)
    except:
        return '{"data":"Connection Failure"}'
    data = ""

    try:
        message = str(command)
        sock.sendall(message.encode('UTF-8'))

        data = sock.recv(104857600)
        #print('got data {0}'.format(data))
        
    finally:
        sock.close()

    if data != "":
        return data.decode('UTF-8')
    else:
        return '{"data":"Connection Failure"}'


def connection_thread(ip, command, thread_list, idx):
    ret_data = fetch_data(ip, command)
    thread_list[idx][1] = ret_data


def start_connection(ip, command, thread_list, idx):
    '''
    This function spins up a thread for the passed ip
    and command and returns the output from the command
    '''
    t = threading.Thread(target=connection_thread, args=(ip,command,thread_list, idx,))
    t.start()
    return t


def poll_machines(packet, vm_num = -1):
    ip_list = ['fa19-cs425-g69-01.cs.illinois.edu',
               'fa19-cs425-g69-02.cs.illinois.edu',
               'fa19-cs425-g69-03.cs.illinois.edu',
               'fa19-cs425-g69-04.cs.illinois.edu',
               'fa19-cs425-g69-05.cs.illinois.edu',
               'fa19-cs425-g69-06.cs.illinois.edu',
               'fa19-cs425-g69-07.cs.illinois.edu',
               'fa19-cs425-g69-08.cs.illinois.edu',
               'fa19-cs425-g69-09.cs.illinois.edu',
               'fa19-cs425-g69-10.cs.illinois.edu']
    
    thread_list = []
    if vm_num == -1:                                                            #if vm_num is -1, add all vms to thread
        for a in range(len(ip_list)):#spawning the threads
            thread_list.append([start_connection(ip_list[a], packet, thread_list, a), "", ip_list[a]])
    else:
        thread_list.append([start_connection(ip_list[vm_num-1], packet, thread_list, 0), "", ip_list[vm_num-1]])
    data_list = [] #what is returned

    for thread in thread_list:
        thread[0].join()
        data_list.append((thread[2], thread[1]))#ip of the machine and the returned data

    return data_list


def request_grep(grep_args):
    packet = {'type': 'grep',
              'args': grep_args}
    data_list = poll_machines(json.dumps(packet))
    for data in data_list:
        print(json.loads(data[1])['data'])

def request_make_test(arg):
    packet = {'type':'gen_test_files', #packet type
              'line_text': str(arg[0]), #the good line to put in with the other randoms
              'random_count': int(arg[1]), #number of random character lines
              'good_count': int(arg[2]), #the count of good lines
              'file_name': str(arg[3])} #the name of the file
    
    poll_machines(json.dumps(packet))

def request_log_query(log_args):
    packet = {'type': 'log',
              'args': log_args}
    data_list = poll_machines(json.dumps(packet), vm_num=int(log_args[1]))
    for data in data_list:
        print(json.loads(data[1])['data'])

def request_halt(arg):
    #halt all service
    if arg == []:
        packet = {'type': 'halt'}
        data_list = poll_machines(json.dumps(packet))
        for data in data_list:
            print(json.loads(data[1])['data'])
    #halt specified service
    else:
        packet = {'type': 'halt'}
        data_list = poll_machines(json.dumps(packet), vm_num=int(arg[0]))
        for data in data_list:
            print(json.loads(data[1])['data'])


def request_put_file(args):

    f_name = args[0]
    sdfs_f_name = args[1]
    print('Requesting put_file of file name {0} with sdfs name {1}'.format(f_name, sdfs_f_name))

    f_path = os.path.join('file', f_name)
    filesize = os.path.getsize(f_path)
    packet = {'type': 'put_file',
              'size': filesize,'filename': sdfs_f_name}
    print('file path: {0}'.format(f_path))
    print('file size : {0}'.format(filesize))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('fa19-cs425-g43-01.cs.illinois.edu', PORT)
    try:
        sock.connect(server_address)
    except:
        return '{"data":"Connection Failure"}'
    data = ""

    print('socket connect')
    try:
        msg = json.dumps(packet).encode()
        print('sending message {0}'.format(msg))
        sock.sendall(msg)
        send_file(f_name, f_path, sock)
        data = sock.recv(1024)
        # print('got data {0}'.format(data))

    finally:
        sock.close()

    if data != "":
        return data.decode('UTF-8')
    else:
        return '{"data":"Connection Failure"}'


def request_get_file(args):

    f_name = args[0]
    print('Requesting get_file with sdfs name {0}'.format(f_name))

    f_path = os.path.join('client', f_name)
    packet = {'type': 'get_file', 'filename': f_name}
    print('file path: {0}'.format(f_path))

#send request to master
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('fa19-cs425-g43-01.cs.illinois.edu', PORT)
    try:
        sock.connect(server_address)
    except:
        return '{"data":"Connection Failure"}'
    data = ""

    print('socket connect')
    try:
        msg = json.dumps(packet).encode()
        print('sending message {0}'.format(msg))
        sock.sendall(msg)
        data = sock.recv(1024)
        data = json.loads(data)
        data = data['response']   #ip
        print('got data {0} from master'.format(data))

    finally:
        sock.close()

    #receive file from the slave
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (data, 22556)
    try:
        sock.connect(server_address)
    except:
        return '{"data":"Connection Failure"}'
    data = ""

    print('socket connect')
    try:
        msg = json.dumps(packet).encode()
        print('sending message {0}'.format(msg))
        sock.sendall(msg)
        data = sock.recv(1024)
        data = json.loads(data)
        size = data['response']
        recv_file(f_name,size, f_path, sock)
    finally:
        sock.close()
    filesize = os.path.getsize(f_path)
    print('Received {0}  size {1}'.format(f_name,filesize))
    return


def recv_file(f_name,file_size, f_path,conn):
    with open(f_path, "wb") as fp:   #fix
        # read chunk by chunk with tqdm to avoid memory exhaustion
        print("Receiving file: {} from client having size: {}".format(f_name, file_size))
        progress = tqdm.tqdm(range(file_size), \
                                "Receiving {}".format(f_name), \
                                unit="B", \
                                unit_scale=True, \
                                unit_divisor=1024)
        recv_len = 0
        for _ in progress:
            data = conn.recv(min(4096, file_size - recv_len))
            recv_len += len(data)
            if len(data) != fp.write(data):
                print("Error writing to file")
                break
            progress.update(len(data))
            if recv_len == file_size:
                break
    return 1 if recv_len == file_size else 0


def send_file(f_name, f_path, conn):
    filesize = os.path.getsize(f_path)
    # ack = {"type": "ack"}
    # ack["response_code"] = 0
    # ack["response"] = filesize
    # conn.sendall(json.dumps(ack).encode())
    print('send file')
    with open(f_path, "rb") as fp:
        # send chunk by chunk with tqdm to avoid memory exhaustion
        print("Sending file: {} to client having size: {}".format(f_name, filesize))
        progress = tqdm.tqdm(range(filesize), \
                             "Sending {}".format(f_name), \
                             unit="B", \
                             unit_scale=True, \
                             unit_divisor=1024)

        send_len = 0
        for _ in progress:

            data = fp.read(min(4096, filesize - send_len))

            conn.sendall(data)
            send_len += len(data)

            progress.update(len(data))
            del data  # hopefully release memory

            if send_len == filesize:
                # read file and send success
                return

def request_del_file(args):
    f_name = args[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(50)
    try:
        request = {"type": "del_file", 'filename':f_name}
        sock.connect(( "fa19-cs425-g43-01.cs.illinois.edu", 1234))
        print('connect')

        sock.sendall(json.dumps(request).encode())  # timeout applies, but should not do harm
        data = sock.recv(1024)
        data = json.loads(data)
        print(data)
        sock.close()
    finally:
        return


def request_ls_file(args):
    f_name = args[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(50)
    try:
        request = {"type": "ret_file_ls", 'filename':f_name}
        sock.connect(( "fa19-cs425-g43-01.cs.illinois.edu", 1234))
        print('connect')
        sock.sendall(json.dumps(request).encode())  # timeout applies, but should not do harm
        data = sock.recv(1024)
        data = json.loads(data)
        if data['response_code'] == 0:
            ip_list = data['response']['vm']
            sorted(ip_list)
            print(ip_list)
        else:
            print(data)
        sock.close()
    finally:
        return


def parse_args():
    description = '''
    Used to grep on files across multiple machines\n
    * grep (use the tool)
    * make_test (create files on all the machines)
    '''

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('command', help="The command that the client should run", type=str)
    args, unknown = parser.parse_known_args()

    if args.command == 'grep':
        request_grep(unknown)
    elif args.command == 'make_test':
        request_make_test(unknown)
    elif args.command == 'log':
        request_log_query(unknown)
    elif args.command == 'halt':
        request_halt(unknown)
    elif args.command == 'put':
        request_put_file(unknown)
    elif args.command == 'get':
        request_get_file(unknown)
    elif args.command == 'del':
        request_del_file(unknown)
    elif args.command == 'ls':
        request_ls_file(unknown)
    else:
        print("unrecognized command try running with --help for a list of commands")


if __name__ == '__main__':
    # logging = logger('client','log/client.log').logger
    parse_args()
    







