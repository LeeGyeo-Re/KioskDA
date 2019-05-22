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
    """
    DB에 있는 User collection중 uuid와 맞는 데이터 가져옴
    """
    user = userCollection.find({
        "uuid": uuid
    })
    return user

def getUserId():
    """
        DB에 있는 User collection 가져옴
    """
    return userCollection.find()

def getRecord(uuid):
    """
        DB에 있는 record Usercollection 가져옴(최근 50개)
    """
    records = recordCollection.find({
        "uuid": uuid
    })
    return records[:50]

def mergeUser(uuid):
    """
    User ID와 record 목록을 가져와서 필요한 데이터 셋으로 합침 
    """
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
    """
    유저가 처음 시작유저인지 판단 
    """
    if getRecord(uuid):
        return False
    else:
        return True

def getNotInterested(uuid):
    """
    notInterested 가져옴 
    """
    interest = interestColeection.find({
        'uuid': uuid
    })
    return interest

class UserData(object):
    """
    해당 클래스는 받아온 유저 데이터를 리스트에 저장하기 위한 목적으로 resultArr에 저장
    makeUserList()는 resultArr을 갱신하는 함수(정기적으로)
    """
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
scheduler.add_job(func=UD.makeUserList, trigger="interval", seconds=3600)   #seconds로 분기 시간을 조정할 수 있음
scheduler.start()
if __name__ == '__main__':
    print("Starting webapp...")
    app.run()
