#! /usr/bin/env python3

import polars as pl
from pathlib import Path


def main():

    input_path = Path.cwd() / 'output'
    input_filename = 'posts.parquet'
    input_filepath = input_path / input_filename
    df = pl.read_parquet(input_filepath)

    df.columns
    cols = range(2, df.shape[1]-2)
    list(range(2, df.shape[1]-2))
    cols = [2, 3, 4, 5, 7, 11, 18, 20]
    df[:, cols]



if __name__ == '__main__':
    main()
