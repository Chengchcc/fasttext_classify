import time, re
from elasticsearch import helpers
from utils import connect_elasticserach, load_yaml_confg
file_path='./data/cooking.train'


def gen():
    config = load_yaml_confg("./conf.yml")
    # data
    es_host = config["data"]["es_host"]
    es_port = config["data"]["es_port"]
    es_password = config["data"]["es_password"]
    es_username = config["data"]["es_username"]
    es_index = config["data"]["es_index"]
    es = connect_elasticserach(es_host,es_port, es_username, es_password)
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
                "_index": es_index,
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
