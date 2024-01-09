# YACS Coding
<h2>Folder Structure</h2>
 - src

    - Master.py
  
    - Worker.py
  
    - Analysis.py
    
    - config.json and requests.py 
  
-logs

    - log_RR.txt
  
    - log_LL.txt
  
    - log_RA.txt
  

<h2>Steps to run:</h2>

- Run the Master.py with command 
 ```python
    $ python3 Master.py config.json <Scheduling Algo(RR/LL/RA)>
  ```
 - Run three workers with the following commands on three different terminals
 ```python
    $ python3 Worker.py 4000 1
    $ python3 Worker.py 4001 2
    $ python3 Worker.py 4002 3
 ```
 - Then run the requests.py to generate task queue
 ```python
    $ python3 requests.py <no_of_requests>
 ```
- 3 log files are then created and Analysis.py helps in analysing them. To run it
 ```python
    $ python3 Analysis.py
 ```
 
 #The logs folder contains logs generated in our tests. U could as well use them to process the analysis.py.
