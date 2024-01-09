import sys
import json
import time
import copy
import random
import socket
import threading as th
from datetime import datetime
from operator import itemgetter

# --------------------------------------------------------------------------------------------------------------------------------------------

global log
global algo
log = 'log_'+str(sys.argv[2])+'.txt'
try:
    algo = sys.argv[2].upper()
except:
    pass
# -------------------------------------------------------------------Variables-----------------------------------------------------------
'''
ConfigData - Contains data from Config.json
request_data - Dict containing key's as requests and values as job id's
ports - List containing ports used 
DictJobSockets - dict with key value as socket id and value as job sockets
req_race_lock - To prevent race conditions while receviving requests
log_race_lock - to prevent race conditions while writing to log files
config_race_lock - to prevent race conditions while reading config data
'''

RequestsData = {}
req_race_lock = th.Lock()
log_race_lock = th.Lock()
config_race_lock = th.Lock()
DictJobSockets = {}
ports = []
ConfigData = 0

# -------------------------------------------------------------Functions for Algorithms--------------------------------------------------------------------------------------------------

'''
RR_ALGO  - Implements Round Robin Algorithm
LL_ALGO  - Implements Least Loaded Algorithm
Random_ALGO - Implements Rnadom Selection Algorithm
'''


def Random_ALGO():
    work_list_ids = []
    for i in ConfigData:
        work_list_ids.append(i['worker_id'])
    while 1:
        num = random.choice(work_list_ids)  # Choose a random worker
        for i in ConfigData:
            if i['worker_id'] == num:
                if i['slots'] == 0:
                    break
                else:  # Return Port if available
                    return i['port']


def LL_ALGO():
    config_race_lock.acquire()
    localData = []
    for i in ConfigData:
        localData.append(i)
    config_race_lock.release()
    # Sorts the list in the dESCENDING order of worker ID
    localData = sorted(localData, key=itemgetter("slots"), reverse=True)

    while 1:
        if(localData[0]['slots'] != 0):
            return localData[0]['port']
        else:
            time.sleep(1)
            config_race_lock.acquire()
            localData = []
            for i in ConfigData:
                localData.append(i)
            config_race_lock.release()
            localData = sorted(
                localData, key=itemgetter("slots"), reverse=True)


def RR_ALGO():
    config_race_lock.acquire()
    localData = []
    for i in ConfigData:
        localData.append(i)
    config_race_lock.release()
    # Sorts the list in the Ascending order of worker ID
    localData = sorted(localData, key=itemgetter("worker_id"))

    while 1:
        for i in localData:
            if (i['slots'] != 0):
                return i['port']
        config_race_lock.acquire()
        localData = []
        for i in ConfigData:
            localData.append(i)
        config_race_lock.release()
        localData = sorted(localData, key=itemgetter("worker_id"))


def chooseAlgo(algo):
    if(algo == 'RR'):
        return RR_ALGO()

    elif(algo == 'LL'):
        return LL_ALGO()

    elif(algo == 'RA'):
        return Random_ALGO()
    else:
        print("Enter one of RR-LL-RA")


# -----------------------------------------------------------------Connective Functions----------------------------------------------------
'''
1) master2worker_send - Send the tasks from master to workers
2) request2master_jobs - Receive the jobs from request.py to Master
3) worker2master_listen - Listen for any request/response sent by workers as an ACK 
4) mapCompletion_check - Check if all the map tasks have been finished 
5) masterWorker_connect - Establish a Connection between master and worker machines
'''


def master2worker_send(job):
    for i in job:
        portNumber = chooseAlgo(algo)  # ChooseAlgo returns the port number
        # portToConnect contains the port to be connected
        portToConnect = ports.index(portNumber)
        pu = portToConnect
        if isinstance(i, dict):
            tid = i["task_id"]
            tid_s = tid.split('_')
            stroc = str(portToConnect + 2)
            Client2Socket, _ = DictJobSockets["soc" + str(stroc)].accept()
            Message = json.dumps(i)
            Client2Socket.send(Message.encode())
            '''
            Decrease the number of possible accumulative slots
            '''
            for j in ConfigData:
                if (j["port"] == portNumber):
                    config_race_lock.acquire()
                    j["slots"] -= 1
                    config_race_lock.release()
                    break

            log_race_lock.acquire()
            StanTime = datetime.now().strftime('%H:%M:%S')
            with open(log, "a") as logfile:
                # status code - 11 - start to assign tasks to workers
                log_11 = []
                log_11.append('11')
                log_11.append(StanTime)
                log_11.append(tid_s[0])
                log_11.append(tid_s[1])
                log_11.append(str(pu+1))
                logfile.write(str(log_11))
                logfile.write('\n')
            log_race_lock.release()

        else:
            pass


def request2master_jobs():
    while True:
        Client2Socket, _ = DictJobSockets["soc0"].accept()
        Message = Client2Socket.recv(1024)
        '''
        Receive the job requests
        '''
        Message = Message.decode("utf-8")
        Message = json.loads(Message)
        map = Message['map_tasks']
        job_id = Message['job_id']
        reducerMessage = Message['reduce_tasks']

        req_race_lock.acquire()
        RequestsData[job_id] = []
        RequestsData[job_id].extend(
            (len(map), len(reducerMessage), reducerMessage))

        req_race_lock.release()

        log_race_lock.acquire()
        StanTime = datetime.now()
        StanTime = StanTime.strftime('%H:%M:%S')

        with open(log, 'a') as logfile:
            # status code - 10 for map tasks start
            list_10 = []
            list_10.append('10')
            list_10.append(StanTime)
            list_10.append(job_id)
            list_10.append(None)
            list_10.append(None)
            logfile.write(str(list_10))
            logfile.write('\n')

        log_race_lock.release()

        print('\n')
        '''
        Check for any leftover Map Tasks,Dispatch Them and Begin the Reduce Tasks
        '''
        if(len(map) != 0):
            print('\t\t\t--------------------->> Job_ID = ',
                  job_id, " <<---------------------\n")
            master2worker_send(map)


def worker2master_listen():
    global ConfigData
    while True:
        Client2Socket, _ = DictJobSockets["soc1"].accept()

        Message = Client2Socket.recv(1024)
        Message = Message.decode("utf-8")
        if(len(Message) > 0):
            Message = json.loads(Message)  # task completed by worker
            tid = Message["task_id"]
            tid_s = tid.split('_')
            log_race_lock.acquire()
            StanTime = datetime.now()
            StanTime = StanTime.strftime('%H:%M:%S')

            with open(log, 'a') as logfile:
                log_20 = []
                log_20.append('20')
                log_20.append(StanTime)
                log_20.append(tid_s[0])
                log_20.append(tid_s[1])
                log_20.append(None)
                # 20 to check for map tasks and start reduce tasks
                logfile.write(str(log_20))
                logfile.write('\n')
            log_race_lock.release()

            linkport = int(Message['linkport'])
            print("--> Received update from worker: ",
                  Message, " from the port", linkport)
            for i in ConfigData:
                if((i["port"]) == linkport):  # EMpty the slot of the worker
                    config_race_lock.acquire()
                    i["slots"] += 1
                    config_race_lock.release()
                    break

            job_id = Message['task_id'].split("_")[0]
            mtid = str(Message['task_id'].split("_")[1][0])
            if mtid == "M":
                ind = 0
            else:
                ind = 1
            req_race_lock.acquire()
            RequestsData[job_id][ind] -= 1
            if RequestsData[job_id][1] == 0:
                log_race_lock.acquire()
                StanTime = datetime.now().strftime('%H:%M:%S')

                with open(log, 'a') as logfile:  # status code - 21 for end of reduce tasks
                    log_21 = []
                    log_21.append('21')
                    log_21.append(StanTime)
                    log_21.append(job_id)
                    log_21.append(None)
                    log_21.append(None)
                    logfile.write(str(log_21))
                    logfile.write('\n')
                log_race_lock.release()

                print(
                    "\t\t\t******************************************************")

                print("\t\t\t\t\t    Job %s has Completed" % (job_id))

                print(
                    "\t\t\t******************************************************\n")

            req_race_lock.release()


def mapCompletion_check():
    iteratedKeys = set()
    while 1:
        keys = set(RequestsData.keys()) - iteratedKeys
        for i in list(keys):
            if RequestsData[i][0] == 0:
                iteratedKeys.add(i)
                master2worker_send(RequestsData[i][2])


def masterWorker_connect(i, j):
    while 1:
        try:
            time_2 = datetime.now()
            time_2 = time_2.strftime("%H:%M:%S")
            print('#### Ready to Establish A Connection ',
                  time_2, " on Port with Index ", str(j + 2), "####")
            Client2Socket, _ = DictJobSockets["soc" + str(j + 2)].accept()
            print("#### Connection Established ####")
            Message = json.dumps(i['slots'])
            Client2Socket.send(Message.encode())
            return
        except:
            continue


def master2worker_init():
    init_list = []
    for i in range(3):
        init_list.append(th.Thread(target=masterWorker_connect,
                                   args=(config_data_original[i], i)))

    for k in range(len(init_list)):
        init_list[k].start()

    for j in range(len(init_list)):
        init_list[j].join()

# --------------------------------------------------------------------Driver Function--------------------------------------------------------


def main():
    if (len(sys.argv) != 3):
        print("--------------------------!Error!-> Usage: python3 Master.py <config.json> <Algorithm>---------------------------------------")
        exit()
    fh = open(sys.argv[1], "r")
    log_race_lock.acquire()
    logfile = open(log, "a+")
    logfile.write("Algo used is "+algo+"\n")
    logfile.write("Format is [code,time,jobid,M/none,W/none]"+'\n')
    logfile.close()
    log_race_lock.release()
    global ConfigData
    global config_data_original
    config_data_original = []

    data = json.load(fh)
    ConfigData = data['workers']
    for i in ConfigData:
        config_data_original.append(i)
        ports.append(i['port'])

    for i in range(5):
        DictJobSockets["soc" +
                       str(i)] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    DictJobSockets["soc0"].bind(('', 5000))    # Ready for incoming jobs

    DictJobSockets["soc0"].listen(5)

    DictJobSockets["soc1"].bind(('', 5001))    # ready to worker updates

    DictJobSockets["soc1"].listen(5)

    for i in range(2, 5):

        DictJobSockets["soc" +
                       str(i)].bind((socket.gethostname(), ports[i - 2]))
        for j in config_data_original:
            if j['port'] == ports[i - 2]:

                number = int(j['slots'])  # assigning the connections
                DictJobSockets["soc" + str(i)].listen(number)
                break

    master2worker_init()

    '''
    Assigning a different thread to each of the task
    Then Starting each thread using start
    Finally joining the threads
    '''
    thr_0 = th.Thread(target=request2master_jobs)
    thr_1 = th.Thread(target=worker2master_listen)
    thr_2 = th.Thread(target=mapCompletion_check)

    thr_0.start()
    thr_1.start()
    thr_2.start()

    thr_0.join()
    thr_1.join()
    thr_2.join()


if __name__ == "__main__":
    main()
