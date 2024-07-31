#! /usr/bin/env python3

import re
import polars as pl
from typing import Any
from pathlib import Path
from dataclasses import dataclass
from validators import url as valid_url


@dataclass
class Post:
    filename: str
    content: list[str]
    tags: list[str]


def remove_unwanted_characters(input_string):
    """
    Remove characters from string that are not alphanumeric or among the 
        specified punctuation characters
    """

    # regex pattern to match characters that are not alphanumeric or among the 
    #   specified punctuation characters
    # pattern = r'[^a-zA-Z0-9_\- ]'
    pattern = r'[^a-zA-Z0-9_ ]'

    # replace matched characters with an empty string
    cleaned_string = re.sub(pattern, '', input_string)

    return cleaned_string


def convert_post_to_markdown(post_info: dict[str, Any]) -> Post:
    """
    Extract and convert information about a website post so that it can be 
        saved as a markdown file
    """

    title = post_info['post_title']
    date_sep = '-'
    date = post_info['post_date'].strftime(f'%Y{date_sep}%m{date_sep}%d')


    # construct the markdown filename
    ##################################################

    # remove the title prefix "What I Read:" or "What I Watch:"
    title_1 = title.split(':')[1].strip().lower()
    title_2 = remove_unwanted_characters(title_1)
    title_3 = title_2.split(' ')
    # put first 2 words of the title into the filename
    title_4 = '_'.join(title_3[:2])

    filename = date + '_' + title_4 + '.md'


    # save the tags for the post
    ##################################################

    unit = post_info['unit']
    tags_str = unit[-1]
    tags = tags_str.split(',')
    tags = [e.strip() for e in tags]
    tags = [e for e in tags if e]
    tags_str = str(tags)


    # assemble the markdown content
    ##################################################

    md_post = []
    md_post.append('+++')
    md_post.append("title = '{}'".format(title.replace("'", "")))
    md_post.append("date = '{}'".format(date))
    md_post.append('\n')
    md_post.append('[taxonomies]')
    md_post.append(f'tags = {tags_str}')
    md_post.append(f"categories = ['repost']")
    md_post.append('+++')
    md_post.append('\n')

    for i, e in enumerate(unit):
        if i < len(unit) - 1:
            if valid_url(e):
                md_post.append(f'[{e}]({e})')
            else:
                md_post.append(e)


    # final assembly
    ##################################################

    post = Post(filename=filename, content=md_post, tags=tags)

    return post


def write_list_to_text_file(
    a_list: list[str], text_filename: Path | str, overwrite: bool=False):
    """
    Writes a list of strings to a text file
    If 'overwrite' is 'True', any existing file by the name of 'text_filename'
        will be overwritten
    If 'overwrite' is 'False', list of strings will be appended to any existing
        file by the name of 'text_filename'

    :param a_list: a list of strings to be written to a text file
    :param text_filename: a string denoting the filepath or filename of text
        file
    :param overwrite: Boolean indicating whether to overwrite any existing text
        file or to append 'a_list' to that file's contents
    """

    if overwrite:
        append_or_overwrite = 'w'
    else:
        append_or_overwrite = 'a'

    with open(text_filename, append_or_overwrite, encoding='utf-8') as txt_file:
        for e in a_list:
            txt_file.write(str(e))
            txt_file.write('\n')


def main():
    """
    Convert the website post data to markdown files
    """

    output_path = Path.cwd() / 'output'
    output_md_path = output_path / 'md_posts'
    output_md_path.mkdir(exist_ok=True, parents=True)

    input_filename = 's03_website_textfile_merged.parquet'
    input_filepath = output_path / input_filename 

    df = pl.read_parquet(input_filepath)

    posts = []

    for row in df.iter_rows(named=True):
        post = convert_post_to_markdown(row)
        posts.append(post)

    for post in posts:

        output_filepath = output_md_path / post.filename

        # some markdown formats/readers display new lines when 2 spaces are at 
        #   the end of a line, but those spaces disrupt Zola static site 
        #   generator
        post.content = [e + '  ' if '+++' not in e else e for e in post.content]

        write_list_to_text_file(post.content, output_filepath, True)


if __name__ == '__main__':
    main()
