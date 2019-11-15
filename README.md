Step 1
Run Redis Server by executing redis-server.exe

Step 2
```
pip install -r requirements.txt
```
Start python server app.py 

Step 3
Start Celery server 
```
celery -A app.celery worker --loglevel=info -P eventlet
```
** if errorin starting then try setting 
```
os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')
```
and then 
```
celery -A app.celery worker --loglevel=info 
```
make sure your selery worker is running .... 
wait until Y=you see
```
[2019-11-15 10:27:09,748: INFO/MainProcess] Connected to redis://localhost:6379//
[2019-11-15 10:27:10,762: INFO/MainProcess] mingle: searching for neighbors
[2019-11-15 10:27:14,801: INFO/MainProcess] mingle: all alone
[2019-11-15 10:27:19,827: INFO/MainProcess] celery@Laptop_001 ready.
``` 
```
** note : <app>.<celery> -->  here app is your python application name, & celery refers the object instance of celery class created. 
```

TESTING

POST 
```
curl http://localhost:5000/starttask
```
GET
```
curl -X POST http://localhost:5000/starttask/90b22520-8d03-454e-9a31-669f57a9c742
```

Output:

![./snapshots/2.png](./snapshots/2.png)
![./snapshots/3.png](./snapshots/3.png)
![./snapshots/4.png](./snapshots/4.png)
![./snapshots/5.png](./snapshots/5.png)
