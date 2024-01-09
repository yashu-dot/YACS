import sys
import time
import json
import socket
import threading as th
from datetime import datetime
portsent = int(sys.argv[1])


def send2master(msg):
    while 1:
        try:
            s1 = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)          # port 5001
            s1.connect((socket.gethostname(), 5001))
            msg['linkport'] = portsent
            msg = json.dumps(msg)
            s1.send(msg.encode())
            s1.close()
            break
        except Exception as e:
            print("Error --%s-- could not bind port" % (e))


def processTask(msg):
    tim = msg['duration']
    while(tim):
        time.sleep(1)
        tim = tim - 1
    print("The task %s has been completed" % (msg['task_id']))


def recvFrmMaster():
    while 1:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            t = datetime.now()
            t = t.strftime("%H:%M:%S")  
            print('Attempting to make a connection to Master', t)
            s.connect((socket.gethostname(), portsent))
            print('Master has connected')
            msg = s.recv(1024)

            if(len(msg) > 0):
                msg.decode("utf-8")
                msg = json.loads(msg)
                print("\n............................................Task has been received from Master............................................\n")
                if(len(msg) != 0):
                    print("Message : ", msg)
                s.close()
                processTask(msg)
                send2master(msg)
        except Exception as e:
            print("Error --%s-- could not bind port" % (e))


def Recv_init():
    while 1:
        try:
            #Fetch the slots
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            time_2 = datetime.now()
            time_2 = time_2.strftime("%H:%M:%S")  # port 4000, 4001, 4002
            print('Attempting to make a connection at time', time_2)
            s.connect((socket.gethostname(), portsent))
            print('Established a Connection')
            s_no = s.recv(1024)
            print("............................................Connection Succesful............................................")
            s.close()
            return s_no.decode("utf-8")
        except Exception as e:
            print("Error --%s-- could not bind port" % (e))


if __name__ == "__main__":
  
    slots = int(Recv_init())
    print("Availabe Slots with Worker are: ", slots)
    
    slotThreads = []
    for i in range(slots):
        slotThreads.append(th.Thread(target=recvFrmMaster))

    for i in range(slots):
        slotThreads[i].start()

    for i in range(slots):
        slotThreads[i].join()
