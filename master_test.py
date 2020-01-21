#!/usr/bin/python3
import os
import sys
import shutil
import json
import random
import socket
import tqdm

'''
#
# message json field definition
# type: message type, present in all messages, can be the following value (string)
#		ret_file_ls	: Message for requesting retrieve file list (including version)
#		put_file	: Message for requesting put file
#		get_file	: Message for requesting retrieve certain file
#		del_file	: Message for requesting delete certain file
#		get_master	: Message for requesting retrieve current master ID on the node
#		halt		: Message for halting slave
#		ack			: Message for responding to requests
#		election	: Message for electing new master (does not have ack back)
#		elected		: Message for notifying end of election (does not have ack back)
# 
# filename: target file w.r.t. this request, present in put_file, get_file, del_file,
# 			value is sdfsfilename for the target file
#
# size: target file size w.r.t. this request, present in put_file,
# 			value is the target file size
#
# version: target file version w.r.t this request, present in put_file,
#			value is the target file version
#
# response_code: response code w.r.t. to the request, "always" present in ack
#			value is 0 for success and 1 for failure
#
# response: answer to the requests, present in ack
#			value dependent on corresponding requests:
#			ret_file_ls	: A json storing filenames and version numbers [a list of dictionary having filename and version] 
# 			put_file	: No such field in ack
# 			get_file	: file size for the requested file
# 			del_file	: No such field in ack
#			get_master	: ID for sdfs master
# 			halt		: No such field in ack
#			
# candidate: target election node w.r.t. this election, present in election and elected,
# 			value is current highest ID for sdfs node
#
# initiator: node that started the election, present in election and elected, 
# 			value is ID for initiating sdfs node
#
'''


class slave_tester:
    def __init__(self, command):
        self.command = command
        self.slave_port = 22556

    #
    # receive all json data from socket
    #
    def recv_json_from_sock(self, sock):
        # sock	: tcp socket from which to retrieve json data

        # use stack to match {} to see if received all json data
        stack = 0
        data = []
        start_flag = True
        while True:
            tmp = sock.recv(1).decode("UTF-8")
            # check if received any data
            if len(tmp) != 1:
                return -1

            data.append(tmp)

            if not start_flag and tmp == '{':
                stack += 1
            elif tmp == '}':
                stack -= 1
            elif start_flag:
                # assuming that the message is always a dumped dictionary, thus starting with {
                stack += 1
                start_flag = False

            # all large paranthesises matched, done
            if stack == 0:
                break

        return "".join(data)

    #
    # run command
    #
    def run(self):
        # construct request message
        request = {}
        if self.command[0] == "ret_file_ls":
            request["type"] = "ret_file_ls"

        elif self.command[0] == "put_file":
            request["type"] = "put_file"
            request["filename"] = self.command[2]
            request["size"] = os.path.getsize(self.command[1])
            request["version"] = int(self.command[3])

        elif self.command[0] == "get_file":
            request["type"] = "get_file"
            request["filename"] = self.command[1]

        elif self.command[0] == "del_file":
            request["type"] = "del_file"
            request["filename"] = self.command[1]

        elif self.command[0] == "get_master":
            request["type"] = "get_master"

        elif self.command[0] == "halt":
            request["type"] = "halt"
        else:
            print("Usage: {} COMMAND [<ARGS>]".format(sys.argv[0]))
            print("ret_file_ls")
            print("put_file localfilename sdfsfilename version")
            print("get_file sdfsfilename localfilename")
            print("del_file sdfsfilename")
            print("get_master")
            print("halt")
            return

        # send command and receive ack
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.settimeout(5)
        try:
            sock.connect(("localhost", self.slave_port))
        except socket.timeout:
            print("Connection timeout")
            return
        except ConnectionRefusedError:
            print("Connection refused")
            return

        # may send and receive large amount of data, disable timeout
        sock.settimeout(None)

        sock.sendall(json.dumps(request).encode())
        if self.command[0] == "put_file":
            # put_file command sends data after requests sent
            with open(self.command[1], "rb") as fp:

                # send chunk by chunk with tqdm to avoid memory exhaustion
                print("Sending file: {} to slave having size: {}".format(request["filename"], request["size"]))
                progress = tqdm.tqdm(range(request["size"]), \
                                     "Sending {}".format(request["filename"]), \
                                     unit="B", \
                                     unit_scale=True, \
                                     unit_divisor=1024)

                send_len = 0
                for _ in progress:

                    data = fp.read(min(4096, request["size"] - send_len))

                    sock.sendall(data)
                    send_len += len(data)

                    progress.update(len(data))
                    del data  # hopefully release memory

                    if send_len == request["size"]:
                        # read file and send success
                        break

        ack = self.recv_json_from_sock(sock)
        if ack == -1:
            print("Error when receiving ack")
            return

        # deal with ack
        ack = json.loads(ack)
        print("Ack: {}".format(ack))
        if ack["response_code"] != -1 and request["type"] == "get_file":
            # get_file command sends data after requests sent
            with open(self.command[2], "wb") as fp:

                # read chunk by chunk with tqdm to avoid memory exhaustion
                print("Receiving file: {} from client having size: {}".format(request["filename"], ack["response"]))
                progress = tqdm.tqdm(range(ack["response"]), \
                                     "Receiving {}".format(request["filename"]), \
                                     unit="B", \
                                     unit_scale=True, \
                                     unit_divisor=1024)

                recv_len = 0
                for _ in progress:
                    # recv from socket and write chunk
                    data = sock.recv(min(4096, ack["response"] - recv_len))
                    recv_len += len(data)

                    if len(data) != fp.write(data):
                        print("Error writing to file")
                        break

                    progress.update(len(data))
                    del data  # hopefully release memory

                    if recv_len == ack["response"]:
                        # recv and write to file success
                        break

        sock.close()

        return


if __name__ == "__main__":
    tester = slave_tester(sys.argv[1:])
    tester.run()