import datetime as dt

import pyspark as ps
from pyspark.sql.functions import desc, explode

from myLib import *

class Validator():
    def __init__(self, df, user_df, logger, dcut):
        self.logger = logger
        dcutdt = dt.date(*[int(elem) for elem in dcut.split('/')])
        latestdt = dt.date(2005,12,31)
        testdays = int((latestdt - dcutdt).days/4)
        testcut = latestdt - dt.timedelta(days=testdays)
        self.train_df = df.where(df.date >  testcut)
        self.test_df  = df.where(df.date <= testcut)
        self.train_df = self.train_df.union(user_df)

    def validate(self, model, write_path):
        self.logger.info('starting makeReccs')
        self.makeReccs(model)
        mscores = self.jointestandrecs(self.recs)

        self.logger.info('starting getPoplr')
        self.getPoplr()
        pscores = self.jointestandpop(self.popmovies)

        self.logger.info('starting performanceResultsViz')
        self.ms, self.ps = performanceResultsViz(mscores, pscores, write_path)

    def makeReccs(self, model):
        subSet = self.test_df.select('user').dropDuplicates()
        self.recs = model.recommendForUserSubset(subSet, 100)

    def jointestandrecs(self, recs):
        recs = recs.select(recs.user, explode('recommendations'))
        recs = recs.rdd.map(lambda x:(x[0], x[1][0], x[1][1]))
        recs = recs.toDF(['user', 'item', 'modelrating'])
        s = ('user', 'item', 'rating')
        recs =self.test_df.select(*s).join(recs,['user','item'], 'inner')
        recs = recs.rdd.map(compare_rates)
        r = recs.groupByKey().mapValues(lambda x:(sum(x)/len(x)))\
        .map(lambda x:x[1]).collect()
        return r

    def getPoplr(self):
        self.popmovies = self.train_df.groupBy('item')\
        .agg({'user':'count'})
        ordrr = self.popmovies['count(user)']
        self.popmovies = self.popmovies.orderBy(ordrr.desc()).limit(100)

    def jointestandpop(self, pop):
        pop = self.test_df.join(pop, 'item', 'inner')
        pop = pop.rdd.map(comparePop)
        r = pop.groupByKey().mapValues(lambda x: (sum(x)/len(x)))\
        .map(lambda x:x[1]).collect()
        return r

if __name__ == '__main__':
    movie0, movie1, movie2 = 'The Fifth Element - 1997', 'Kids in the Hall: Brain Candy - 1996', 'Batman - 1989'
    rating0, rating1, rating2 = 4.0, 4.0, 4.0
