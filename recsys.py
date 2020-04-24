import re, json ,time, fasttext
from utils import load_yaml_confg, connect_elasticserach, load_fasttext_model

old_precision = 0

def fastext_train(train_file, valid_file, output):
    model = fasttext.train_supervised(
        input=train_file, lr=1.0, label='__label__', epoch=25, wordNgrams=2, dim=200, bucket=200000, loss='hs')
    res = model.test(valid_file)
    global old_precision
    if res[1] > old_precision:
        old_precision = res[1]
        model.save_model(output)
    return model



def train_job():
    config = load_yaml_confg("./conf.yml")
    # model
    fasttext_model = config['model']['fasttext_model']
    train_data_path = config['model']['train_data_path']
    valid_data_path = config['model']['valid_data_path']
    # data
    es_host = config["data"]["es_host"]
    es_port = config["data"]["es_port"]
    es_password = config["data"]["es_password"]
    es_username = config["data"]["es_username"]
    es_index = config["data"]["es_index"]
    print("es: %s:%d" %(es_host, es_port))

    es = connect_elasticserach(es_host, es_port, es_username, es_password)

    query_body = {
        "query": {
            "function_score": {
                "query": {
                    "match_all": {}
                },
                "script_score" : {
                    "script" : {
                        "source": "Math.log(2 + doc['orderId'].value)"
                    }
                }
            }
        },
        "size":10000
    }

    res = es.search(body=query_body, index=es_index)
    with open(train_data_path, 'wt') as ft, open(valid_data_path, 'wt') as fv:
        count = len(res['hits']['hits'])

        for idx, data in enumerate(res['hits']['hits']):
            source = data['_source']
            labels = list(source['tags'])
            labels_str = ' '.join(['__label__'+label for label in labels])
            content = source['desc']
            if idx< count*0.8:
                ft.write(labels_str+' '+re.sub(r'[^\w\s]', '', content).strip().lower()+'\n')
            else:
                fv.write(labels_str+' '+re.sub(r'[^\w\s]', '', content).strip().lower()+'\n')
    ft.close()
    fv.close()
    fastext_train(train_data_path, valid_data_path, fasttext_model)

if __name__ == "__main__":
    train_job()



