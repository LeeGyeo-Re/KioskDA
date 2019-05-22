from flask import Flask
from pymongo import MongoClient
import threading
from apscheduler.schedulers.background import BackgroundScheduler

connection = MongoClient("mongodb://master:master1@ds147746.mlab.com:47746/app_7")    #mongodb 주소 입력
db = connection['app_7']             #mongodb database 입력
userCollection = db['users']             #collection입력
recordCollection = db['records']
interestColeection =  db['interest']

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


def getUser(uuid):
    user = userCollection.find({
        "uuid": uuid
    })
    return user

def getUserId():
    return userCollection.find()

def getRecord(uuid):
    records = recordCollection.find({
        "uuid": uuid
    })
    return records[:50]

def mergeUser(uuid):
    result = {}
    user = getUser(uuid)
    user = list(user)
    user = user[0]
    records = getRecord(uuid)
    records = list(records)
    if records:
        records = records[0]

    result['uuid'] = uuid
    if "age" in user:
        result['age'] = user['age']
    if "gender" in user:
        result['gender'] = user['gender']
    if len(records) is 0:
        result['likes'] = user['likes']
        result['count'] = 1
    else:
        result['tid'] = list(records)
        result['count'] = records.count()
    return result

def startUser(uuid):
    if getRecord(uuid):
        return False
    else:
        return True

def getNotInterested(uuid):
    interest = interestColeection.find({
        'uuid': uuid
    })
    return interest

class UserData(object):

    def __init__(self):
        self.resultArr = []

    def makeUserList(self):
        """
        데이터 값 받아서 리스트로 만들어줌
        """
        self.resultArr = []
        for ii in getUserId():
            id = ii['uuid']
            temp = []
            user = mergeUser(id)
            temp.append(user['uuid'])
            if startUser(id):
                temp.append(user['tid'])
            else:
                temp.append(user['likes'])
            temp.append(user['count'])
            self.resultArr.append(temp)

UD = UserData()


@app.route('/recommand')
def recommand():
    return "분석된 데이터 전송"

scheduler = BackgroundScheduler()
scheduler.add_job(func=UD.makeUserList, trigger="interval", seconds=3600)
scheduler.start()
if __name__ == '__main__':
    print("Starting webapp...")
    app.run()
