# A script that pulls DOI from any journal publication website
import requests
from bs4 import BeautifulSoup
import re
import argparse
import pandas as pd
import sys


def pull_doi(url: str):
    """Scrapes the DOI for a paper connected with a given URL.
    Args:
        url : str
            The URL of the paper to be scraped.
    Returns:
        str
            A string which is either empty or containing the paper's DOI.
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    r = requests.get(url, headers=headers)
    html = r.text
    soup = BeautifulSoup(html, 'html.parser')
    doi = ''

    for a in soup.find_all('a', href=True):
        link = a['href']
        if 'doi.org' in link:
            doi = link.split('doi.org/')[1]
            break

        # Pulling first DOI from text
        # DOI not embedded in a href, but can only be pulled by searching the text
    if len(doi) == 0:
        doi = soup(text=re.compile(r'/^10.\d{4,9}/[-._;()/:A-Z0-9]+$/i'))

        if len(doi) == 0:
            doi = re.search(
                r'\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\'<>])\S)+)\b', url).groups()

        if len(doi):
            doi = doi[0].strip()
            # removing all characters before first number in DOI
            doi = re.search('[0-9].*', doi)[0]
        else:
            doi = ""
    return doi


def merge_dois(algolia_df: pd.DataFrame):
    """Looks through the AskNature papers and scrapes the DOI where needed.
    Args:
        algolia_df : pd.DataFrame
            A Pandas DataFrame containing our formatted AskNature papers.
    Returns:
        pd.DataFrame
            A modified Pandas DataFrame of the input DataFrame where each DOI has been attempted to be filled.
    """

    size = algolia_df.shape[0]
    for index, row in algolia_df.iterrows():
        print("{:0.2%}".format(index/size))
        if not row.get("doi", False):
            continue

        url = row["url"]

        try:
            doi = pull_doi(url)
        except Exception as error_type:

            if (error_type == KeyboardInterrupt):
                break

            doi = ""

        if doi:
            doi = re.sub(r'%', "/", doi)
            algolia_df.at[index, "doi"] = doi

    return algolia_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Pull DOI from Any Journal Website')
    parser.add_argument('algolia_papers', type=str,
                        help='File path of algolia paper csv')
    args = parser.parse_args()
    alg = pd.read_csv(args.algolia_papers).astype({"doi": "string"})
    alg = merge_dois(alg)
    alg.to_csv("doi_scraped_papers.csv", index=False)
