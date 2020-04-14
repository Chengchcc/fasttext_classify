import re, json ,time, fasttext
from utils import load_yaml_confg, connect_elasticserach, load_fasttext_model

def fastext_train(file, output):
    model = fasttext.train_supervised(
        input=file, lr=1.0, label='__label__', epoch=25, wordNgrams=2, dim=200, bucket=200000, loss='hs')
    model.save_model(output)
    return model



def train_job():
    config = load_yaml_confg("./conf.yml")
    # model
    fasttext_model = config['model']['fasttext_model']
    tmp_data_path = config['model']['tmp_data_path']
    # data
    es_host = config["data"]["es_host"]
    es_port = config["data"]["es_port"]
    es_password = config["data"]["es_password"]
    es_username = config["data"]["es_username"]
    print("es: %s:%d" %(es_host, es_port))

    es = connect_elasticserach(es_host, es_port, es_username, es_password)

    query_body = {
        "query": {"range": {
            "@timestmap": {
                "gte": 10
            }
        }},
        "size":  12400
    }

    res = es.search(body=query_body, index="recsys", doc_type='goodsdata')
    with open(tmp_data_path, 'wt') as f:
        for data in res['hits']['hits']:
            source = data['_source']
            labels = [source['PTY_1'], source['PTY_2'], source['PTY_3']]
            if 'null' in labels:
                labels.remove('null')
            labels_str = ' '.join(['__label__'+label for label in labels])
            content = source['ITEM']
            f.write(labels_str+' '+re.sub(r'[^\w\s]', '', content).strip().lower()+'\n')
    f.close()
    fastext_train(tmp_data_path, fasttext_model)

if __name__ == "__main__":
    train_job()



