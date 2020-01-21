# CS425 MP3 README

### Overall Process
1. Create 5 local directory named "client", "file", "log", "master" and "tmp"
2. Start up the server process (SDFS nodes) on VM01, join the server process (using the "introducer" command)
3. Start up and join all the other server processes on other VMs (using the "join" command)
4. Start up the slave thread on each SDFS nodes (using the "start sdfs" command)
5. Put files that will be put into the SDFS in the "file" directory and issue request with the client program. Perform leave and join of the SDFS nodes directly in the SDFS node commandline.
6. [Optional] Start up the MP1 programs as stated in MP1 README for remote log grepping (the log for slave is named "slave_log.log", the log for master is named "master_log.log". They are both stored in the same directory as the 5 directories created previously)
> Note that for the server on VM01 (introducer) to rejoin after leaving/failing, the server on VM02 must be alive and have joined the system

> Note that for the other server process to rejoin after leaving/failing, the server on VM01 (introducer) must be alive and have joined the system 

### Compile and Run the Code
#### SDFS node
To run the SDFS node, use the following command:
```
python3 sdfs_slave.py
```
The SDFS node will then provide a commandline interface where one can issue commands for the membership list components (including showing membership list, joining and leaving) and also start the SDFS slave thread.

A list of available commands are shown as follows:
1. introducer: For the introducer of the SDFS to join the system, only used when starting up the SDFS. Note that when the introducer left the system and rejoins later, one should use the "join" command rather than the "introducer" command to rejoin.
2. join: For other SDFS nodes to join the system.
3. leave: For leaving the system.
4. membership list: For showing the current membership list.
5. membership list id: For showing the current membership list in IDs.
6. self id: For showing self ID.
7. get table: Shows the heartbeat sending and receiving table.
8. failure list: Deprecated, should not be used.
9. reverse list: Deprecated, should not be used.
10. exit: For exiting the program after leaving the system.
11. start sdfs: For starting the slave thread of the SDFS node, should only be called once. (Note that the commandline will prompt you with "If master, input 1. Else 0", just enter 0 for all nodes joining the system)
    
Normal execution of the SDFS node is as follows:
Start the SDFS node by running the above command, then
    1. If it is the first SDFS node, it should be run on VM01. use "introduce" to join it. If their are already nodes in the system and the introducer on VM01 is alive, join using "join"
    2. After all nodes have joined the system, use "start sdfs" command to start the slave thread of the SDFS node (afterwards, any joining of a new node can be done directly by using the "join" and then the "start sdfs" commands).

#### Client Program
To run the client program, use the following command:
```
python3 client_cmd_line.py
```
The client program will then provide a commandline interface where one can issue requests to the SDFS (including put, get, del, ls and store as required in the demo instruction), provided as follows:

1. get \<sdfs file name> \<local file name>
2. put \<local file name> \<sdfs file name>
3. del \<sdfs file name>
4. store \<vm number>
5. ls \<sdfs file name>

The functionalities of these commands are the same as those specified in the MP spec. Note that the "store" command allows checking the list of files stored at each VM for all VMs by specifying the VM number.

Every local file of the client is stored in the <projectroot>/file directory.
The retrieved SDFS file is stored in the <projectroot>/client directory.