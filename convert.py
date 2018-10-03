import pandas as pd
import s3fs
from dateparser.date import DateDataParser
from dotenv import load_dotenv
from datetime import datetime, timedelta
from csv import QUOTE_ALL
import re, json, logging
from multiprocessing import Pool
from argparse import ArgumentParser

quote = re.compile(r'"')

class Parser():
    def __init__(self, languages = ['en']):
        self.parser = DateDataParser(languages=languages)

    def _parse(self, s):
        return self.parser.get_date_data(s).get('date_obj')

    def parse_date(self, d,t):
        try:
            delta = datetime.now() - self._parse(d)
            truncated = False
        except TypeError:
            delta = timedelta(days = 30)
            truncated = True
        scraped = self._parse(t)
        return datetime.date(scraped - delta), truncated

def parse_date(df):
    parser = Parser()
    p = Pool()
    res = p.starmap(parser.parse_date, zip(df.date, df.scrapeTimestamp))
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

parser = ArgumentParser('Convert JL')
parser.add_argument('folders', nargs='+')

if __name__ == '__main__':
    args = parser.parse_args()
    fs = s3fs.S3FileSystem()
    for folder in args.folders:
        infiles = fs.ls(folder)
        outfiles = [make_outfile(f, 'indeed-csvs') for f in infiles]
        for i,o in zip(infiles, outfiles):
            logging.info('CONVERTING: {}'.format(i))
            convert(i,o,fs)
