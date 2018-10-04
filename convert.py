import pandas as pd
import s3fs
from dateparser.date import DateDataParser
from dotenv import load_dotenv
from datetime import datetime, timedelta
from csv import QUOTE_ALL
import re, json, logging
from multiprocessing import Pool, cpu_count
from argparse import ArgumentParser
from functools import lru_cache, partial
from itertools import starmap

quote = re.compile(r'"')

class Parser():
    def __init__(self, languages = ['en']):
        self.parser = DateDataParser(languages=languages)

    @lru_cache(maxsize=256)
    def _parse(self, s):
        return self.parser.get_date_data(s).get('date_obj')

    def parse_date(self, d,t):
        try:
            delta = datetime.now() - self._parse(d)
            truncated = False
        except TypeError:
            delta = timedelta(days = 30)
            truncated = True
        scraped = datetime.fromisoformat(t)
        return datetime.date(scraped - delta), truncated

def parse_date(df):
    parser = Parser()
    res = starmap(parser.parse_date, zip(df.date, df.scrapeTimestamp))
    dates, truncated = zip(*res)
    return df.assign(date = dates, date_truncated = truncated)

def clean_df(df):
    return (df
            .assign(
                category = df.categories.map(lambda l: l[0]),
                subcategory = df.categories.map(lambda l: l[1]),
                description = df.description.str.replace(quote, ''))
            .pipe(parse_date)
            .drop(['categories', 'meta', 'reviews'], 1, errors='ignore'))

def convert(infile, outfile, fs):
    with fs.open(infile, 'r') as f:
        df = pd.DataFrame([json.loads(l) for l in f.readlines()])
    with fs.open(outfile, 'w') as f:
        clean_df(df).to_csv(f, index=False, quoting=QUOTE_ALL)

def make_outfile(infile, folder, i = 1):
    parts = infile.split('/')
    parts.insert(i, folder)
    return re.sub('.jl', '.csv', '/'.join(parts))

parser = ArgumentParser('Convert JL')
parser.add_argument('folders', nargs='+')
parser.add_argument('--cores', default=cpu_count())
parser.add_argument('--outpath', default='indeed-csvs')

if __name__ == '__main__':
    load_dotenv()
    args = parser.parse_args()
    fs = s3fs.S3FileSystem()
    pool = Pool(int(args.cores))
    for folder in args.folders:
        logging.info('CONVERTING: {}'.format(folder))
        infiles = fs.ls(folder)
        outfiles = [make_outfile(f, args.outpath) for f in infiles]
        pool.starmap(partial(convert, fs=fs), zip(infiles, outfiles))
    pool.join()
    pool.close()
