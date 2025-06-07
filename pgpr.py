import gzip
from mapper import write_time_based_train_test_split
from mapper import map_to_PGPR
import os
import subprocess
import argparse


dataset_name = "ml1m"

write_time_based_train_test_split(dataset_name, "pgpr", 0.8)

with gzip.open("data/ml1m/preprocessed/pgpr/train.txt.gz", 'rt') as train_file:
    for i, line in enumerate(train_file):
        if i > 5: break
        print(line)
train_file.close()

map_to_PGPR(dataset_name)

with gzip.open("data/ml1m/preprocessed/pgpr/editor.txt.gz", 'rt') as editor_file:
    for i, line in enumerate(editor_file):
        if i > 5: break
        print(line)
editor_file.close()

with gzip.open("data/ml1m/preprocessed/pgpr/category_p_ca.txt.gz", 'rt') as belong_to_file:
    for i, line in enumerate(belong_to_file):
        if i > 5: break
        print(line)
belong_to_file.close()

os.chdir("models/PGPR")



parser = argparse.ArgumentParser()
parser.add_argument('--max_path_len', type=int, default=3, help='Max path length.')
parser.add_argument('--act_dropout', type=float, default=0.3, help='action dropout rate.')
args = parser.parse_args()

# print(f"pgpr.py\nmax_path_len: {args.max_path_len}, act_dropout :{args.act_dropout}")
def run_file(file_name,args):
    try:
        # result = subprocess.run(
        #     ["python3", file_name, 
        #     "--dataset", dataset_name, 
        #     "--max_path_len", str(args.max_path_len)] + 
        #     (file_name == "train_agent.py")*["--act_dropout", str(args.act_dropout)
        #     ],
        #     capture_output=True,
        #     text=True,
        #     check=True
        # )

        result = subprocess.run(
            ["python3", file_name] + (args if args else []),
            capture_output=True,
            text=True,
            check=True
        )

        # Print output to ensure it gets logged
        
        print("", result.stdout)
        # print("process Errors (if any):\n", result.stderr)
        
    except subprocess.CalledProcessError as e:
        print(f"Error while running {file_name}: {e}")
        print("Output:\n", e.output)
        print("Error Output:\n", e.stderr)




# print("Step 1")

run_file(file_name="preprocess.py",args=["--dataset",dataset_name])


# print("Step 2")

run_file(file_name="train_transe_model.py",args=["--dataset",dataset_name])


# print("Step 3")

run_file(file_name="train_agent.py",args=["--dataset",dataset_name,"--max_path_len",str(args.max_path_len),"--act_dropout",str(args.act_dropout)])


# print("Step 4")

run_file(file_name="test_agent.py",args=["--dataset",dataset_name,"--max_path_len",str(args.max_path_len)])

os.chdir("../..")