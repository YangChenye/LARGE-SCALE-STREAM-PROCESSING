# Created by Zhuoyue Xing on 2020/5/6.
# Copyright © 2020 Zhuoyue Xing. All rights reserved.
from flask import Flask,Response
from flask import jsonify
from flask import request
import json
from flask_pymongo import PyMongo


app = Flask(__name__)
app.config['MONGO_DBNAME']= 'E6889final'
app.config['MONGO_URI']= 'mongodb://localhost:27017/E6889final'
# put name of database at the end of URL
mongo = PyMongo(app)
_host = "0.0.0.0"
_port = 8099

#get all data from dataset
@app.route('/6889final/getall', methods=['GET'])
def get_all():
    location = mongo.db.E6889final
    output = []
    for c in location.find():
        output.append({'time': c['time'],'func1':c['func1'],'func2':c['func2'],'func3':c['func3'],'func4':c['func4'], 'func5' :c['func5'], 'func6': c['func6']})
    return jsonify({'result' : output})

#insert data to dataset
@app.route('/6889final/insert', methods=['POST'])
def insert_by_sensornumber():
    result = None
    try:
        if request.method == 'POST':
            data = None
            try:
                if request.data is not None:
                    data = request.json
                else:
                    data = None
            except Exception as e:
                # This would fail the request in a more real solution.
                data = "You sent something but I could not get JSON out of it."
            location = mongo.db.E6889final
            location.insert_one(data)
            res = "Insert successfully"
            rsp = Response(json.dumps(res), status=200, content_type="application/json")
            return rsp
        else:
            result = "Invalid request."
            return result, 400, {'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        print(e)
        return handle_error(e, result)


@app.route('/6889final/deleteall', methods=['DELETE'])
def delete_6889final_all():
    result = None
    try:
        if request.method == 'DELETE':
            location = mongo.db.E6889final
            location.remove({})
            res = "Delete all successfully"
            rsp = Response(json.dumps(res), status=200, content_type="application/json")
            return rsp
        else:
            result = "Invalid request."
            return result, 400, {'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        print(e)
        return handle_error(e, result)


@app.route('/6889final/getrecent1', methods=['GET'])
def get_recent_6889final():
    data = mongo.db.E6889final
    output = []
    for c in data.find().sort([('time', -1)]).limit(1):
        output.append({'time': c['time'],'func1':c['func1'],'func2':c['func2'],'func3':c['func3'],'func4':c['func4'], 'func5' :c['func5'], 'func6': c['func6']})
    return jsonify({'result' : output})


def handle_error(e, result):
    return "Internal error.", 504, {'Content-Type': 'text/plain; charset=utf-8'}

if __name__ == '__main__':
    app.run()
