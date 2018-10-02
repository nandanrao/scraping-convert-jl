import pandas as pd
import s3fs
import dateparser
from dotenv import load_dotenv
from datetime import datetime, timedelta
from csv import QUOTE_ALL
import re, json, logging
from multiprocessing import Pool

FOLDERS = [
    'oecd-scraping/indeed-philippines',
    'oecd-scraping/indeed-india',
    'oecd-scraping/indeed-us',
    'oecd-scraping/indeed-uk'
]

quote = re.compile(r'"')

def _parse_date(d,t):
    try:
        delta = datetime.now() - dateparser.parse(d)
        truncated = False
    except TypeError:
        delta = timedelta(days = 30)
        truncated = True
    scraped = dateparser.parse(t)
    return datetime.date(scraped - delta), truncated

def parse_date(df):
    p = Pool()
    res = p.starmap(_parse_date, zip(df.date, df.scrapeTimestamp))
    dates, truncated = zip(*res)
    return df.assign(date = dates, date_truncated = truncated)

def clean_df(df):
    return (df
            .assign(
                category = df.categories.map(lambda l: l[0]),
                subcategory = df.categories.map(lambda l: l[1]),
                description = df.description.str.replace(quote, ''))
            .pipe(parse_date)
            .drop(['categories', 'meta', 'reviews'], 1))

def convert(infile, outfile, fs):
    with fs.open(infile, 'r') as f:
        df = pd.DataFrame([json.loads(l) for l in f.readlines()])
    with fs.open(outfile, 'w') as f:
        clean_df(df).to_csv(f, index=False, quoting=QUOTE_ALL)

def make_outfile(infile, folder, i = 1):
    parts = infile.split('/')
    parts.insert(i, folder)
    return re.sub('.jl', '.csv', '/'.join(parts))

if __name__ == '__main__':
    load_dotenv()
    fs = s3fs.S3FileSystem()
    for folder in FOLDERS:
        infiles = fs.ls(folder)
        outfiles = [make_outfile(f, 'indeed-csvs') for f in infiles]
        for i,o in zip(infiles, outfiles):
            logging.info('CONVERTING: {}'.format(i))
            convert(i,o,fs)
