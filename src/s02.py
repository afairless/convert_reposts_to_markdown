#! /usr/bin/env python3

import re
import polars as pl
from pathlib import Path
from ast import literal_eval
from bs4 import BeautifulSoup as bs


def find_urls_in_string(a_string: str):
    """
    Adapted from:
        https://stackoverflow.com/questions/9760588/how-do-you-extract-a-url-from-a-string-using-python
    """

    regex = r'('
    # Scheme (HTTP, HTTPS, FTP and SFTP):
    regex += r'(?:(https?|s?ftp):\/\/)?'
    # www:
    regex += r'(?:www\.)?'
    regex += r'('
    # Host and domain (including ccSLD):
    regex += r'(?:(?:[A-Z0-9][A-Z0-9-]{0,61}[A-Z0-9]\.)+)'
    # TLD:
    regex += r'([A-Z]{2,6})'
    # IP Address:
    regex += r'|(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    regex += r')'
    # Port:
    regex += r'(?::(\d{1,5}))?'
    # Query path:
    regex += r'(?:(\/\S+)*)'
    regex += r')'
    
    find_urls_in_string = re.compile(regex, re.IGNORECASE)
    urls = find_urls_in_string.findall(a_string)

    return urls


def remove_ad_hoc_extraneous_text_from_url(a_string: str) -> str:
    
    extra_text = [
        'Penrose: From Mathematical Notation to Beautiful Diagrams',
        'Learning Neural Causal Models from Unknown InterventionsNan Rosemary Ke, Olexa Bilaniuk, Anirudh Goyal, Stefan Bauer, Hugo Larochelle, Chris Pal, Yoshua Bengio',
        'Structural Time SeriesFF16 · October 2020',
        'NeRF Explosion 2020Published: December 16, 2020',
        'The Modern Stack for ML Infrastructure | OuterboundsMay 2, 2022Data Council',
        'How to Freaking Find Great Developers By Having Them Read CodeApril 15, 2022 by freakingrectangle',
        'AI And The Limits Of LanguageBy Jacob Browning and Yann LeCunAugust 23, 2022',
        'Avenging Polanyi\'s Revenge (Exploiting the Approximate Omniscience of LLMs in Planning..)Subbarao KambhampatiAug 1, 2023"…we have to stop confusing the impressive form of the generated knowledge for correct content, and resist the temptation to ascribe emergent reasoning powers to approximate retrieval by these n-gram models…"',
        'Computational Power and AIBy Jai Vipra & Sarah Myers WestSeptember 27, 2023',
        'Thinking about High-Quality Human DataLilian WengFebruary 5, 2024',
        'Defining optimal reliance on model predictions in AI-assisted decisionsJessica Hullman3/6/24 12:31 PM']

    for e in extra_text:
        a_string = a_string.replace(e, '')

    return a_string


def remove_extraneous_text_from_youtube_url(a_string: str) -> str:
    """
    Some YouTube URLs have extraneous text after the video ID; this function
        removes that extraneous text
    """

    if 'youtu' in a_string:
        a_string = a_string.split('?si')[0]

    return a_string


def extract_url_from_string_dict_given_start_idx(
    a_string: str, start_idx: int, dict_key: str='url') -> str:

    txt = a_string[start_idx:]
    end_idx = txt.find('}')
    txt = txt[:end_idx+1]
    try:
        url_dict = literal_eval(txt)
    except:
        txt = txt.replace('true', '"true"')
        url_dict = literal_eval(txt)
    url = url_dict[dict_key]

    return url


def extract_url_from_string_dict(a_string) -> list[str]:
    """
    Extracts URL from a string with an embedded dictionary

    Example string:
        'This is {"url": "https://www.example.com"} a string'
    Example dictionary:
        {"url": "https://www.example.com"}
    """

    start_txt = '{"url":'
    start_idx_1 = a_string.find(start_txt)
    start_idx_2 = a_string.rfind(start_txt)

    if start_idx_1 != -1:
        if start_idx_1 == start_idx_2:
            url = extract_url_from_string_dict_given_start_idx(
                a_string, start_idx_1)
            return [url]
        else:
            url_1 = extract_url_from_string_dict_given_start_idx(
                a_string, start_idx_1)
            url_2 = extract_url_from_string_dict_given_start_idx(
                a_string, start_idx_2)
            return [url_1, url_2]
    else:
        return ['']


def extract_urls_from_post_content(
    post_content: pl.Series) -> list[str | list[str]]:
    """
    Extracts URLs from a Series of post content, i.e., HTML from a webpage
    """

    extracted_urls = []

    for content_txt in post_content:

        soup = bs(content_txt, 'html.parser')

        url = None

        # 3 ways to extract URLs  
        #   if earlier one(s) don't work, try next one
        ##################################################

        # Extraction #1
        soup_as = soup.find_all('a')
        urls = [a['href'] for a in soup_as]
        if len(urls) >= 1:
            url = urls[0]

        # Extraction #2
        if not url:
            soup_ps = soup.find_all('p')
            soup_ps_urls = [e.text for e in soup_ps if 'http' in e.text]
            if len(soup_ps_urls) >= 1:
                txt = soup_ps_urls[0]
                txt = remove_ad_hoc_extraneous_text_from_url(txt)
                url = txt

        # Extraction #3
        if not url:
            url_list = extract_url_from_string_dict(content_txt)
            if len(url_list) > 1:
                url = url_list
            elif len(url_list) == 1 and url_list[0]:
                url = url_list[0]
            else:
                url = ''

        if isinstance(url, str):
            url = remove_extraneous_text_from_youtube_url(url)

        extracted_urls.append(url)

    return extracted_urls


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

    post_urls = extract_urls_from_post_content(df3['post_content'])
    assert len(post_urls) == len(df3)


    df2[:, col_idxs]
    df3[:, col_idxs]
    df3['post_status'].value_counts()
    df3['post_title'].n_unique()
    df3['post_name'].n_unique()
    df3['post_content'].n_unique()
    df3['post_type'].value_counts()



    for e in post_urls:
        print(e)


    # post_status:  future has date when scheduled to be published
    # post_status:  'publish' apparently same as 'future'
    # post_status:  inherit has date when scheduled



if __name__ == '__main__':
    main()
