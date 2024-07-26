#! /usr/bin/env python3

import gzip
import polars as pl
from pathlib import Path
from ast import literal_eval


def filter_sql_to_correct_table(txt01: list[str]) -> list[str]:
    """
    Extracts the SQL code for the table '_5A5_posts' from the SQL dump file
    """

    start_str = 'INSERT INTO `_5A5_posts` '
    end_str = '-- Table structure for table `_5A5_termmeta`'

    start_idx = None
    end_idx = None
    for i, line in enumerate(txt01):
        if not start_idx and start_str in line:
            start_idx = i
        if start_idx and not end_idx and end_str in line:
            end_idx = i

    txt02 = txt01[start_idx:end_idx]

    j = len(txt02)
    for j in range(len(txt02)-1, 0, -1):
        if ';' in txt02[j]:
            break

    end_idx_2 = j + 1
    txt03 = txt02[:end_idx_2]

    return txt03


def create_posts_dataframe(txt: list[str]) -> pl.DataFrame:
    """
    Recreate the '_5A5_posts' table from the SQL dump file as a DataFrame
    """

    # colnames_txt = txt[0].split('(')[1].split(')')[0].replace('`', '')
    # colnames = colnames_txt.split(',')

    rows01 = [e for e in txt if e[0] == '(' and 'insert into' not in e.lower()]
    rows02 = [e[:e.rfind(')')+1] for e in rows01]
    rows03 = [literal_eval(e) for e in rows02]

    for e in rows03:
        assert isinstance(e, tuple)

    schema = {
        'ID': pl.Int64, 'post_author': pl.Int64, 'post_date': pl.Utf8, 
        'post_date_gmt': pl.Utf8, 'post_content': pl.Utf8, 
        'post_title': pl.Utf8, 'post_excerpt': pl.Utf8, 'post_status': pl.Utf8, 
        'comment_status': pl.Utf8, 'ping_status': pl.Utf8, 
        'post_password': pl.Utf8, 'post_name': pl.Utf8, 'to_ping': pl.Utf8, 
        'pinged': pl.Utf8, 'post_modified': pl.Utf8, 
        'post_modified_gmt': pl.Utf8, 'post_content_filtered': pl.Utf8, 
        'post_parent': pl.Int64, 'guid': pl.Utf8, 'menu_order': pl.Int64, 
        'post_type': pl.Utf8, 'post_mime_type': pl.Utf8, 
        'comment_count': pl.Int64}
    df = pl.DataFrame(rows03, schema=schema, orient='row')

    df2 = df.with_columns(
        pl.col('post_date').str.to_datetime(
            format='%Y-%m-%d %H:%M:%S', strict=False),
        pl.col('post_date_gmt').str.to_datetime(
            format='%Y-%m-%d %H:%M:%S', strict=False),
        pl.col('post_modified').str.to_datetime(
            format='%Y-%m-%d %H:%M:%S', strict=False),
        pl.col('post_modified_gmt').str.to_datetime(
            format='%Y-%m-%d %H:%M:%S', strict=False))
    # df = pd.DataFrame(rows03, columns=colnames)
    # df.columns = colnames

    return df2


def main():
    """
    Extract information about posts from Wordpress website SQL dump file and
        save it to a dataframe
    """

    input_path = Path.cwd() / 'input'
    input_filename = 'localhost.sql.gz'
    input_filepath = input_path / input_filename

    txt = []
    with gzip.open(input_filepath, 'rt') as f:
        for line in f:
            txt.append(line)

    txt01 = filter_sql_to_correct_table(txt)
    df = create_posts_dataframe(txt01)

    output_path = Path.cwd() / 'output'
    output_path.mkdir(exist_ok=True, parents=True)

    output_filename = 's01_posts.parquet'
    output_filepath = output_path / output_filename
    df.write_parquet(output_filepath)

    output_filename = 's01_posts.csv'
    output_filepath = output_path / output_filename
    df.write_csv(output_filepath)


if __name__ == '__main__':
    main()
