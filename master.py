"""
Master script to partition image csv, and submit one job per partition
input_dir expects to have subdirectories:
    /input-dir/csv/ukbb_40606_new_eids.csv
    /input-dir/utils/ukbfetch
    /input-dir/key/ukbb.key
"""

import os
from argparse import ArgumentParser
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List


CWD = Path(__file__).parent


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--input-dir", dest="input_dir", type=str,
                        help="input dir expects to have subdirectories ./csv ./utils ./key")
    parser.add_argument("--n-partition", dest="n_partition", type=int)
    parser.add_argument("--output-dir", dest="output_dir", type=str)
    parser.add_argument("--n-thread", dest="n_thread", type=int, default=0)
    return parser.parse_args()


def partition_csv(csv_path: Path, n_partition: int, output_dir: Path):
    output_dir.mkdir(exist_ok=True, parents=True)
    df = pd.read_csv(str(csv_path))
    dfs = np.array_split(df, n_partition)
    for i, df in enumerate(dfs):
        df.to_csv(str(output_dir.joinpath(f"partition_csv_{i}.csv")), index=False)
    return dfs


def create_batch_files(input_dir: Path, part_csv_files: List[Path], output_dir: Path, n_thread) -> List[Path]:
    job_script_path = CWD.joinpath("job.py")
    python_command = f"python {str(job_script_path)}"
    with open(str(CWD.joinpath("batch_template.txt")), "r") as file:
        sbatch = file.read()
    temp_batch_file_dir = CWD.joinpath("temp")
    temp_batch_file_dir.mkdir(exist_ok=True, parents=True)
    batch_files = []
    for idx, csv_file in enumerate(part_csv_files):
        command = python_command + f" --input-dir {str(input_dir)} --csv-file {str(csv_file)} --output-dir {str(output_dir)}  --n-thread {n_thread}"
        batch = sbatch + f"{command}\n"
        batch_file = temp_batch_file_dir.joinpath("")
        with open(f"job_{idx}.sh", "r") as file:
            file.write(batch)
        batch_files.append(batch_file)
    return batch_files


def submit_batch_files(batch_files: List[Path]):
    for batch_file in batch_files:
        os.system(f"sbatch {str(batch_file)}")


def main():
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    n_partition = args.n_partition
    csv_file = input_dir.joinpath("csv", "ukbb_40616_new_eids.csv")
    part_csv_files = partition_csv(csv_file, n_partition, output_dir.joinpath("temp", "csv"))
    batch_files = create_batch_files(input_dir, part_csv_files, output_dir, args.n_thread)
    submit_batch_files(batch_files)


if __name__ == '__main__':
    main()