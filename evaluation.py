import numpy as np
import pandas as pd
from random import seed, randint, choice
from collections import defaultdict
import pickle
from knowledge_graph_utils import entity2plain_text
import os

dataset_name = "ml1m"

models = ["pgpr"]
users_topk = {model: defaultdict(list) for model in models}

train_labels = {}
test_labels = {}

ndcgs = {model: [] for model in models}
recalls = {model: [] for model in models}
precisions = {model: [] for model in models}

def dcg_at_k(topk, k, method=1):
    topk = np.asfarray(topk)[:k]
    if topk.size:
        if method == 0:
            return topk[0] + np.sum(topk[1:] / np.log2(np.arange(2, topk.size + 1)))
        elif method == 1:
            return np.sum(topk / np.log2(np.arange(2, topk.size + 2)))
        else:
            raise ValueError('method must be 0 or 1.')
    return 0.

def ndcg_at_k(topk, k, method=1):
    dcg_max = dcg_at_k(sorted(topk, reverse=True), k, method)
    if not dcg_max:
        return 0.
    return dcg_at_k(topk, k, method) / dcg_max

def recall_at_k(topk, test_pids):
    return sum(topk) / len(test_pids)

def precision_at_k(topk, k):
    return sum(topk) / k

entity2name = {}
entity2name["pgpr"] = entity2plain_text(dataset_name, "pgpr")

def path_len(path):
    len = 0
    for s in path:
        if type(s) != str:
            s = str(s)
        if s.isnumeric():
            len+=1
    return len

# Path Structure: user 5038 watched product 2430 watched user 1498 watched product 1788
def template(curr_model, path):
    if path[0] == "self_loop":
        path = path[1:]

    path_length = path_len(path)
    for i in range(1, len(path)):
        s = str(path[i])
        if s.isnumeric():
            if path[i-1] == 'user': continue
            if int(path[i]) not in entity2name[curr_model][path[i-1]]: continue
            path[i] = entity2name[curr_model][path[i-1]][int(path[i])]
    if path_length == 4:
        _, uid, rel_0, e_type_1, e_1, rel_1, e_type_2, e_2, rel_k, _, pid  = path
        return f"{pid} is recommend to you because you {rel_0} {e_1} also {rel_k} by {e_2}"
    elif path_len(path) == 3:
        _, uid, rel_0, e_type_1, e_1, rel_1, _, pid  = path
        return f"{pid} is recommend to you because is {rel_1} with {e_1} that you previously {rel_0}"

curr_model = "pgpr"

with open(f"results/{dataset_name}/{curr_model}/pred_paths.pkl", 'rb') as pred_paths_file:
    pred_paths_pgpr = pickle.load(pred_paths_file)
pred_paths_file.close()

# print(pd.DataFrame(pred_paths_pgpr[:10], columns=["uid", "pid", "path_score", "path_probability", "path"]))

header = ["uid", "pid", "path_score", "path_prob", "path"]
pred_paths_map_pgpr = defaultdict(dict)
for record in pred_paths_pgpr:
    uid, pid, path_score, path_prob, path = record
    if pid not in pred_paths_map_pgpr[uid]:
        pred_paths_map_pgpr[uid][pid] = []
    pred_paths_map_pgpr[uid][pid].append((float(path_score), float(path_prob), path))

n_users = len(pred_paths_map_pgpr.keys())
random_user = randint(0, n_users)
random_product = choice(list(pred_paths_map_pgpr[random_user].keys()))
# print(pred_paths_map_pgpr[random_user][random_product])

# print(os.getcwd())

os.chdir("models/PGPR")

# print(os.getcwd())

from models.PGPR.pgpr_utils import load_labels

train_labels[curr_model] = load_labels(dataset_name, 'train')
test_labels[curr_model] = load_labels(dataset_name, 'test')

os.chdir("../..")

# print(os.getcwd())

best_pred_paths = {}
for uid in pred_paths_map_pgpr:
    if uid in train_labels[curr_model]:
        train_pids = set(train_labels[curr_model][uid])
    else:
        print("Invalid train_pids")
    best_pred_paths[uid] = []
    for pid in pred_paths_map_pgpr[uid]:
        if pid in train_pids:
            continue
        # Get the path with highest probability
        sorted_path = sorted(pred_paths_map_pgpr[uid][pid], key=lambda x: x[1], reverse=True)
        best_pred_paths[uid].append(sorted_path[0])

random_user = randint(0, n_users)
# print(best_pred_paths[random_user][:5])

n=10

no_of_rec = []

for uid in range(len(best_pred_paths.keys())):
    sorted_paths = sorted(best_pred_paths[uid], key=lambda x: (x[0], x[1]), reverse=True)
    
    no_of_rec.append(len(sorted_paths))
    
    sorted_paths = [[path[0], path[1], path[-1].split(" ")] for path in sorted_paths]
    
    topk_products = [int(path[-1][-1]) for path in sorted_paths[:n]]
    topk_explanations = [path[-1] for path in sorted_paths[:n]]
    users_topk[curr_model][uid] = list(zip(topk_products, topk_explanations))
print(f"Average no. of recomendations: {np.mean(no_of_rec)}")
# print(no_of_rec)
random_user = randint(0, n_users)
users_topk[curr_model][random_user][0]

for uid, rec_exp_tuples in users_topk[curr_model].items():
    hits = []
    for rec_exp_tuple in rec_exp_tuples:
        recommended_pid = rec_exp_tuple[0]
        if recommended_pid in test_labels[curr_model][uid]:
            hits.append(1)
        else:
            hits.append(0)
    while len(hits) < 10:
        hits.append(0)
    ndcg = ndcg_at_k(hits, n, 0)
    precision = precision_at_k(hits, n)
    recall = recall_at_k(hits, test_labels[curr_model][uid])
    ndcgs[curr_model].append(ndcg)
    precisions[curr_model].append(precision)
    recalls[curr_model].append(recall)
print(f"Overall NDGC: {np.mean(ndcgs[curr_model])}, Precision: {np.mean(precisions[curr_model])}, Recall: {np.mean(recalls[curr_model])}")

random_user = randint(0, len(users_topk[curr_model].keys()))
for i, pid_exp_tuple in enumerate(users_topk[curr_model][random_user]):
    pid, exp = pid_exp_tuple[0], pid_exp_tuple[1]
    users_topk[curr_model][random_user][i] = (pid, template(curr_model, exp))

# print(pd.DataFrame(users_topk[curr_model][random_user], columns=["pid", "textual explanation"]))
