#! /usr/bin/env python3

import polars as pl
from pathlib import Path


def main():

    input_path = Path.cwd() / 'output'
    input_filename = 'posts.parquet'
    input_filepath = input_path / input_filename
    df = pl.read_parquet(input_filepath)

    assert (df[:, 2] == df[:, 3]).all()
    assert (df['post_date'] == df['post_date_gmt']).all()

    # col_idxs = [2, 4, 5, 7, 11, 18, 20]

    # filter by "What I Read/Watch"
    df2 = df.filter(
        pl.col('post_type').eq('post') &
        pl.col('post_title').str.to_lowercase().str.starts_with('what i'))

    # 'post_type' column at index 20 no longer needed
    col_idxs = [2, 4, 5, 7, 11, 18]

    # for the purpose of needing a single copy of each post, 'inherit' is 
    #   redundant
    # 'draft' and 'auto-draft' are also unneeded
    df3 = df2.filter(
        ~pl.col('post_status').eq('inherit') &
        ~pl.col('post_status').str.contains('draft') )

    df2[:, col_idxs]
    df3[:, col_idxs]
    df3['post_status'].value_counts()
    df3['post_title'].n_unique()
    df3['post_name'].n_unique()
    df3['post_content'].n_unique()
    df3['post_type'].value_counts()


    # post_status:  future has date when scheduled to be published
    # post_status:  'publish' apparently same as 'future'
    # post_status:  inherit has date when scheduled




if __name__ == '__main__':
    main()
