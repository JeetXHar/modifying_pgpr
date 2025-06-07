import os
import subprocess

def run_file(file_name, args=None):
    try:
        result = subprocess.run(
            ["python3", file_name] + (args if args else []),
            capture_output=True,
            text=True,
            check=True
        )
        # Print output to ensure it gets logged
        #if file_name == "evaluation.py":
          
        print("", result.stdout)
        # print("process Errors (if any):\n", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error while running {file_name}: {e}")
        print("Output:\n", e.output)
        print("Error Output:\n", e.stderr)

max_len_path = [1,2,3,4,5,6,7]
dropout = [0.1,0.2,0.3,0.4,0.5,0.6,0.7]

# print(max_len_path,dropout)
for l in max_len_path:
    for do in dropout:
        
      run_file("preprocessing_code.py")

      run_file("pgpr.py",["--max_path_len", str(l), "--act_dropout", str(do)])

      print(l,do)
      run_file("evaluation.py")