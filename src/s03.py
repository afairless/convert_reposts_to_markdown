#! /usr/bin/env python3

import polars as pl
from pathlib import Path


def read_text_file(
    text_filename: str | Path, return_string: bool=False, 
    keep_newlines: bool=False):
    """
    Reads text file
    If 'return_string' is 'True', returns text in file as a single string
    If 'return_string' is 'False', returns list so that each line of text in
        file is a separate list item
    If 'keep_newlines' is 'True', newline markers '\n' are retained; otherwise,
        they are deleted

    :param text_filename: string specifying filepath or filename of text file
    :param return_string: Boolean indicating whether contents of text file
        should be returned as a single string or as a list of strings
    :return:
    """

    text_list = []

    try:
        with open(text_filename) as text:
            if return_string:
                # read entire text file as single string
                if keep_newlines:
                    text_list = text.read()
                else:
                    text_list = text.read().replace('\n', '')
            else:
                # read each line of text file as separate item in a list
                for line in text:
                    if keep_newlines:
                        text_list.append(line)
                    else:
                        text_list.append(line.rstrip('\n'))
            text.close()

        return text_list

    except:

        return ['There was an error when trying to read text file']


def insert_missing_urls(df: pl.DataFrame) -> pl.DataFrame:

    match_dict = {
        'What I Read:  Data-efficient image Transformers': 'https://ai.facebook.com/blog/data-efficient-image-transformers-a-promising-new-technique-for-image-classification/',
        'What I Read:  Bayesian Media Mix Modeling': 'https://www.pymc-labs.io/blog-posts/bayesian-media-mix-modeling-for-marketing-optimization/',
        'What I Read:  Bayesian Structural Timeseries': 'https://nathanielf.github.io/post/bayesian_structural_timeseries/',
        'What I Read:  undesired goals': 'https://www.deepmind.com/blog/how-undesired-goals-can-arise-with-correct-rewards',
        'What I Read:  Russian Roulette': 'https://fa.bianp.net/blog/2022/russian-roulette/',
        'What I Read:  How Quickly LLMs Learn Skills?': 'https://www.quantamagazine.org/how-quickly-do-large-language-models-learn-unexpected-skills-20240213/'}

    return df


def main():

    input_path = Path.cwd() / 'input'
    output_path = Path.cwd() / 'output'

    input_filename = '01posts.txt'
    input_filepath = input_path / input_filename
    posts_txt = read_text_file(input_filepath)

    input_filename = 'posts_what_i_read.parquet'
    input_filepath = output_path / input_filename
    df = pl.read_parquet(input_filepath)


    post_units = []
    unit = []
    for e in posts_txt:
        if e.strip():
            unit.append(e)
        else:
            if unit:
                post_units.append(unit)
            unit = []

    post_urls = [e[0] for e in post_units]

    assert len(post_units) == len(post_urls)

    unit_srs = pl.Series(post_units).alias('unit')
    url_srs = pl.Series(post_urls).alias('url')
    posts_df = pl.DataFrame([url_srs, unit_srs])

    df2 = df.with_columns(pl.col('post_urls').list.get(0).alias('url'))

    match_df = pl.DataFrame(
        [('What I Read:  Data-efficient image Transformers', 
          'https://ai.facebook.com/blog/data-efficient-image-transformers-a-promising-new-technique-for-image-classification/'), 
         ('What I Read:  Bayesian Media Mix Modeling', 
          'https://www.pymc-labs.io/blog-posts/bayesian-media-mix-modeling-for-marketing-optimization/'), 
         ('What I Read:  Bayesian Structural Timeseries', 
          'https://nathanielf.github.io/post/bayesian_structural_timeseries/'), 
         ('What I Read:  undesired goals', 
          'https://www.deepmind.com/blog/how-undesired-goals-can-arise-with-correct-rewards'), 
         ('What I Read:  Russian Roulette', 
          'https://fa.bianp.net/blog/2022/russian-roulette/'), 
         ('What I Read:  How Quickly LLMs Learn Skills?', 
          'https://www.quantamagazine.org/how-quickly-do-large-language-models-learn-unexpected-skills-20240213/')], 
        schema=['post_title', 'url'], 
        orient='row')

    df3 = df2.join(match_df, on='post_title', how='left')
    df3.filter(pl.col('url_right').is_not_null())
    df4 = df3.with_columns(
        pl.when(pl.col('url_right').is_not_null())
        .then(pl.col('url_right'))
        .otherwise(pl.col('url')).alias('url')).drop('url_right')
    df2 = df4

    # aa = df2.with_columns(
    #     pl.when(pl.col('post_title').is_in(match_dict.keys()))
    #     # .then(pl.lit('1'))
    #     .then(pl.lit(match_dict[pl.col('post_title')]))
    #     .otherwise(pl.col('url')).alias('url'))
    # aa.filter(pl.col('url').eq('1'))

    # df2.with_columns(
    #     pl.when(pl.col('post_title').is_in(match_dict.keys()))
    #     .then((match_dict[pl.col('post_title')]))
    #     .otherwise(pl.col('url')))

    # some URLs end in a slash or question mark and some don't, so remove them
    # slash_mask = df2['url'].str.ends_with('/') | df2['url'].str.ends_with('?')
    punct_mask = df2['url'].str.contains('[/?]$', literal=False)
    df2 = df2.with_columns(
        pl.when(punct_mask)
        .then(
            pl.col('url')
            .str.slice(0, length=pl.col('url').str.len_chars()-1))
        .otherwise(pl.col('url')))

    # one URL ends in '#nice', so remove it
    hash_mask = df2['url'].str.ends_with('#nice')
    df2 = df2.with_columns(
        pl.when(hash_mask)
        .then(
            pl.col('url')
            .str.slice(0, length=pl.col('url').str.len_chars()-5))
        .otherwise(pl.col('url')))

    # some URLs end in a slash and some don't, so remove them
    slash_mask = posts_df['url'].str.ends_with('/')
    posts_df = posts_df.with_columns(
        pl.when(slash_mask)
        .then(
            pl.col('url')
            .str.slice(0, length=pl.col('url').str.len_chars()-1))
        .otherwise(pl.col('url')))

    # one URL was improperly rendered due to its underscores, so correct it
    url_0 = 'https://thegradient.pub/bootstrapping-labels-via-_-supervision-human-in-the-loop'
    url_1 = 'https://thegradient.pub/bootstrapping-labels-via-___-supervision-human-in-the-loop'
    df2 = df2.with_columns(
        pl.when(pl.col('url').eq(url_0))
        .then(pl.lit(url_1).alias('url'))
        .otherwise(pl.col('url')))


    df3 = df2.join(posts_df, on='url', how='inner')
    dfa = df2.join(posts_df, on='url', how='left')

    df2.shape
    df3.shape

    dfb = dfa.filter(pl.col('unit').is_null())

    for e in dfb.iter_rows(named=True):
        print(e['post_date'])
        print(e['post_title'])
        print(e['url'])
        print('\n')


    for e in post_units:
        print(e)
        print('\n')

    for e in post_urls:
        print(e)


    posts_df.filter(pl.col('url').str.contains('pub/bootstr'))[0, 0]
    df2.filter(pl.col('url').str.contains('pub/bootstr'))[0, -1]

    bb = posts_df['url'].filter(slash_mask)
    cc = aa['url'].filter(slash_mask)

    for i in range(len(bb)):
        print(bb[i])
        print(cc[i])

    df2.filter(pl.col('post_urls').list.len() > 1)[0, -1]
    df2.filter(pl.col('post_urls').list.len() > 1)[0, -2]
    df2.filter(pl.col('post_urls').list.len() > 1)[0, -2][0]
    df2.filter(pl.col('post_urls').list.len() > 1)[0, -2][1]

    df2[0, -1]
    posts_df.filter(pl.col('url').str.contains('nature.com/articles'))[0, 0]



if __name__ == '__main__':
    main()
