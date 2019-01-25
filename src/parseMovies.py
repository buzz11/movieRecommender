import os
import sys
import datetime as dt
from glob import glob

def saveTohdfs(fpath, rows, logger, first=False):
    '''
    https://hdfs3.readthedocs.io/en/latest/api.html
    import hdfs3
    '''
    hdfsDest = '/user/ryan/movieRatings.csv'
    munged_file_name = 'munged_%s' % fpath.split('/')[-1]\
    .replace('.txt','.csv')
    munged_file_path = os.path.abspath(os.path.join(\
    os.getcwd(), __file__, '..', '..', 'data', 'tmp', munged_file_name))
    logger.info(munged_file_path)
    with open(munged_file_path, 'w') as f:
        f.write('\n'.join(rows))
    if first:
        os.system('hdfs dfs -put -f {} {}'\
        .format(munged_file_path, hdfsDest))
    else:
        os.system('hdfs dfs -appendToFile {} {}'\
        .format(munged_file_path, hdfsDest))
    os.system('rm {}'.format(munged_file_path))

def checkForMunged():
    munged = os.path.join(os.path.dirname(os.path.abspath(__file__)),\
    '..', 'data', 'movieratings.csv')
    return os.path.isfile(munged)

def saveToLocal(rows, first=False):
    dest = os.path.join(os.path.dirname(os.path.abspath(__file__)),\
    '..', 'data', 'movieratings.csv')
    mode = 'a'
    if first:
        mode = 'w'
    with open(dest, mode) as f:
        f.write('\n'.join(rows))

def parseMovieData(fpath, cutoff, logger, first=False, toLocal=True):
    elems = [int(elem) for elem in cutoff.split('/')]
    cutoff = dt.date(*elems)
    logger.info('reading {} ... '.upper().format(fpath))
    with open(fpath, 'r') as f:
        rows = f.read().splitlines()
    logger.info('finding movie ids ... '.upper())
    movie_id = None
    new_rows = []
    for row in rows:
        if ':' in row:
            movie_id = row[:-1]
            continue
        dateElems = [int(elem) for elem in row.split(',')[2].split('-')]
        rateDate = dt.date(*dateElems)
        if rateDate > cutoff:
            new_rows.append('%s,%s' % (row, movie_id))
    if first:
        new_rows.insert(0, 'user,rating,date,item')
    if toLocal:
        logger.info('saving {} rows'.upper().format(len(new_rows)))
        saveToLocal(new_rows, first=first)
        return
    logger.info('saving {} rows to hdfs'.upper().format(len(new_rows)))
    saveTohdfs(fpath, new_rows, logger, first=first)

def checkForFiles(logger):
    datadir = os.path.join(os.getcwd(),'..','data')
    if not os.path.isdir(datadir):
        os.makedirs(os.path.abspath(datadir))
    wc = 'combined_data_*'
    exp = [os.getcwd(), '..', 'data', 'netflix-prize-data']
    thing = os.path.abspath(os.path.join(*exp))
    file_paths = glob(os.path.join(thing, wc))
    if len(file_paths) == 0:
        logger.error('download combined_data_*.txt files and movie_titles.csv from https://www.kaggle.com/netflix-inc/netflix-prize-data and save in %s' % thing)
        sys.exit()
    return file_paths

def parseandsave(cutoff, logger, toLocal=True):
    '''
    input: str 'YYYY/MM/DD'
    '''
    file_paths = checkForFiles(logger)
    for n, fpath in enumerate(file_paths):
        if n == 0:
            parseMovieData(fpath, cutoff, logger, first=True, toLocal=toLocal)
        else:
            parseMovieData(fpath, cutoff, logger, toLocal=toLocal)

if __name__ == '__main__':
    cutoff = '2005-12-21'
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(\
	'%(asctime)s- %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    parseandsave(cutoff, logger)
