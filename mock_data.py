import time
from elasticsearch import helpers
from utils import connect_elasticserach
file_path='./data/cooking.train'
host="139.9.250.148"
port=9200
username="XXX"
password="XXX"

def gen():
    es = connect_elasticserach(host,port, username, password)
    actions = []
    with open(file_path, "r", encoding="utf-8") as f:
        i = 0
        label = []
        name = ""
        prefix_len = len("__label__")
        for data in f.readlines():
            line = data.split(" ")
            for buffer in line:
                if buffer.startswith("__label__"):
                    label.append(buffer[prefix_len:])
                else:
                    name += (" "+ buffer)
            if len(label) < 3:
                label.extend(["null" for x in range(3 - len(label))])
            action = {
                "_index": "recsys",
                "_type": "goodsdata",
                "_source": {
                    "ITEM": name,
                    "PTY_1": label[0],
                    "PTY_2": label[1],
                    "PTY_3": label[2],
                    "@timestmap": int(round(time.time()*1000))
                },
            }
            label.clear()
            i+=1
            name = ""
            actions.append(action)
            if len(actions) == 1000:
                helpers.bulk(es, actions)
                del actions[0:len(actions)]
        if i > 0:
            helpers.bulk(es, actions)

if __name__ == "__main__":
    gen()
