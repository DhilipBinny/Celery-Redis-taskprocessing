from flask import Flask, jsonify, url_for
from flask_cors import CORS
from celery_init import make_celery
import random
import time
import os

os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')

app = Flask(__name__)
log = app.logger

CORS(app)

# configure broker and backend
app.config.update(
    CELERY_BROKER_URL='redis://localhost:6379',
    result_backend='redis://localhost:6379'
)
celery = make_celery(app)

path_to_create_folder = r"./jobs/"

# POST request to initiate the longrunningtask 
@app.route('/starttask', methods=['POST'])
def starttask():
    task = longrunningtask.apply_async()
    print(">"*30,task.id)
    os.makedirs(path_to_create_folder+task.id)
    path_to_file = path_to_create_folder+task.id
    with open(path_to_file +"/output.json", "w") as f: 
        f.write(f"Current_work :  UNKNOWN , Total_jobs :  UNKNOWN , STATUS : STARTED ") 

    return jsonify({ "task_id" : task.id})


# GET request along with taskid to check the status of the long running task
@app.route('/starttask/<task_id>')
def taskstatus(task_id):
    task = longrunningtask.AsyncResult(task_id)
    path_to_file = path_to_create_folder+task_id
    with open(path_to_file + "/output.json", "r") as f: 
        response = f.read() 
    return jsonify(response)

# This function is the long running function to be assigned to the worker
@celery.task(name = "app.py.longrunningtask",bind=True)
def longrunningtask(self):
    total = random.randint(50, 100)
    for i in range(total):
        task_id = self.request.id
        path_to_file = path_to_create_folder+task_id
        with open(path_to_file + "/output.json", "w") as f: 
            f.write(f"Current_work : { i }, Total_jobs : { total } , STATUS : PROCESSING ") 

        time.sleep(5)
    
    with open(path_to_file + "/output.json", "w") as f: 
            f.write(f"Current_work : COMPLETED , Total_jobs : { total } , STATUS : COMPLETED ") 

    return {"status":"completed "}


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)


