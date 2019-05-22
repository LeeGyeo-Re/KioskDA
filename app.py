from flask import Flask
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

connection = MongoClient("mongodb://master:master1@ds147746.mlab.com:47746/app_7")    #mongodb 주소 입력
db = connection['app_7']             #mongodb database 입력
userCollection = db['users']             #collection입력
recordCollection = db['records']
interestCollection = db['interest']
print("어디가 문제야")
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'


def getUser(uuid):
    """
    DB에 있는 User collection중 uuid와 맞는 데이터 가져옴
    """
    print("어디가 문제야1")
    user = userCollection.find({
        "uuid": uuid
    })
    print("어디가 문제야1")
    return user

def getUserId():
    """
        DB에 있는 User collection 가져옴
    """
    print("어디가 문제야2")
    return userCollection.find()

def getRecordTid(uuid):
    """
        DB에 있는 record Usercollection 가져옴(최근 50개)
    """
    print("어디가 문제야3")
    records = recordCollection.find({
        "uuid": uuid
    })
    records = list(records)
    if records:
        records = records[0]
    else:
        records = []
    print("어디가 문제야3")
    return records

def countlist(list):
    print("어디가 문제야4")
    dic = {}
    if list:
        return dic
    for i in list:
        if not dic[i]:
            dic[i] = 1
        else:
            dic[i] += 1
    return dic
def mergeUser(uuid):
    """
    User ID와 record 목록을 가져와서 필요한 데이터 셋으로 합침 
    """
    print("어디가 문제야5")
    user = getUser(uuid)
    user = list(user)
    user = user[0]
    records = getRecordTid(uuid)
    resultlist = []
    result = {}
    dicts = {}

    if len(records) is 0:
        dicts = countlist(user['likes'])
    else:
        dicts = countlist(records['tid'])
    for key in dicts.keys():
        result['uuid'] = uuid
        if "age" in user:
            result['age'] = user['age']
        if "gender" in user:
            result['gender'] = user['gender']
        result['tid'] = key
        result['count'] = dicts[key]
        resultlist.append(result)
        print("어디가 문제야5")
    return resultlist

def startUser(uuid):
    """
    유저가 처음 시작유저인지 판단 
    """
    print("어디가 문제야6")
    if getRecordTid(uuid):
        return False
    else:
        return True

def getNotInterested(uuid):
    """
    notInterested 가져옴 
    """
    print("어디가 문제야7")
    interest = interestCollection.find({
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
        print("어디가 문제야8")
        self.resultArr = []
        for ii in getUserId():
            id = ii['uuid']
            user = mergeUser(id)
            for u in user:
                temp = []
                temp.append(u['uuid'])
                temp.append(u['tid'])
                temp.append(u['count'])
                for t in temp:
                    print(t)
                self.resultArr.append(temp)
            print("한줄")

UD = UserData()

scheduler = BackgroundScheduler()
scheduler.add_job(func=UD.makeUserList, trigger="interval", seconds=5)  # seconds로 분기 시간을 조정할 수 있음
scheduler.start()
@app.route('/recommand')
def recommand():
    return "분석된 데이터 전송"


if __name__ == '__main__':
    print("Starting webapp...")
    app.run()
