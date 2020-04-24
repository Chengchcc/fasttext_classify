import time, re
from elasticsearch import helpers
from utils import connect_elasticserach
file_path='./data/cooking.train'
host="139.9.250.148"
port=9200
username="recsys_admin"
password="PLAT4life"

def gen():
    es = connect_elasticserach(host,port, username, password)
    actions = []
    with open(file_path, "r", encoding="utf-8") as f:
        i = 0
        label = []
        name = ""
        prefix_len = len("__label__")
        for data in f.readlines():
            line = re.sub(r'[^\w\s]', '', data).strip().lower().split(" ")
            for buffer in line:
                if buffer.startswith("__label__"):
                    label.append(buffer[prefix_len:])
                else:
                    name += (" "+ buffer)
            action = {
                "_index": "recsys",
                "_source": {
                    "desc": name,
                    "tags":list(label),
                    "orderId": i,
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
