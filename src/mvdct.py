import os
from pyspark.sql.functions import col

class MovieDict():
    def __init__(self, spark, relevantids, logger):
        self.spark = spark
        self.logger = logger
        work_dir = os.path.abspath(os.path.dirname(__file__))
        data_dir = os.path.join(work_dir, '..', 'data')
        ff = 'movie_titles.csv'
        self.fpath = os.path.join(data_dir, 'netflix-prize-data', ff)
        self.logger.info('reading %s' % self.fpath)
        self.movieTitles=self.spark.createDataFrame(self.spark.read.csv(\
        self.fpath, encoding='iso-8859-1').rdd,['id','year','title'])
        self.movieTitles = self.movieTitles.where(col('id')\
        .isin(relevantids))
        self.movieTitles.createTempView('movie_id')
        orderingq = """SELECT title, year FROM movie_id ORDER BY year DESC, title"""
        ordered_options_df = self.spark.sql(orderingq)
        self.options = []
        for tup in ordered_options_df.collect(): #make local dict ?
            if tup[1] == 'NULL':
                self.options.append(tup[0])
            else:
                self.options.append('%s - %s' % (tup[0], tup[1]))
        self.movieset = set(self.options)
        self.logger.info\
        ('number of moviedict options {}'.format(str(len(self.options))))
                # options = ['%s - %s' % (tup[0], tup[1])\
                # for tup in ordered_options_df.collect()\
                # if tup[1] != 'NULL' else str(tup[0])]

    def lookupmovietitles(self, ids):
        '''
        INPUT: set
        OUTPUT: list
        '''
        return ['%s - %s' % (r['title'], r['year']) for r in self.movieTitles.where(col('id').isin(ids)).select('title', 'year').collect()]

    def lookupmovieids(self, mvs):
        '''
        INPUT: set
        OUTPUT: list
        '''
        # q = """SELECT id FROM movie_id WHERE title IN (%s)""" % mvs
        # ids = [r['id'] for r in self.spark.sql(q).collect()]
        return [r['id'] for r in self.movieTitles.where(col('title').isin(mvs)).select('id').collect()]
