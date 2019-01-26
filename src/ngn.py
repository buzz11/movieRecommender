import os
import logging
import math

import pyspark as ps
import datetime as dt
import tmdbsimple as tmdb
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.ml.recommendation import ALS
import pandas as pd

from parseMovies import *
from myLib import *
from mvdct import MovieDict
from vldtr import Validator

class Ratings():
	def __init__(self):
		self.userratings = {}
		self.ratings = []

class Engine():
	def __init__(self, sc, dcut='2005/9/30', sample=False):
		if not sample:
			checkForFiles(self.logger)
		readtextfiles = not checkForMunged()
		self.dcut = dcut
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.INFO)
		formatter = logging.Formatter(\
		'%(asctime)s- %(name)s - %(levelname)s - %(message)s')
		ch = logging.StreamHandler()
		ch.setFormatter(formatter)
		self.logger.addHandler(ch)
		self.spark = ps.sql.SparkSession(sc)
		if readtextfiles and not sample:
			parseandsave(dcut, self.logger, toLocal=True)
		tmdb.API_KEY = os.getenv('movie_v3key')
		if tmdb.API_KEY:
			self.search = tmdb.Search()

		self.schema = StructType([
		StructField("user", IntegerType(), False),
		StructField("rating", IntegerType(), False),
		StructField("date", TimestampType(), True),
		StructField("item", IntegerType(), False)])

		self.wd = os.path.dirname(os.path.realpath(__file__))
		self.data_dir =os.path.abspath(os.path.join(self.wd,'..','data'))

		if sample:
			ratingspath= os.path.join(self.data_dir,
			'sample_movieratings.csv')
			self.logger.info\
			('reading {} ...'.upper().format(ratingspath))
			self.df = self.spark.read.csv(ratingspath, header=True,
			schema=self.schema).dropna()

		else:
			ratingspath=os.path.join(self.data_dir,'movieratings.csv')
			self.logger.info\
			('reading {} ...'.upper().format(ratingspath))
			self.df = self.spark.read.csv(\
			ratingspath, header=True, schema=self.schema).dropna()

		ids = self.df.select('item').dropDuplicates().collect()
		ids = [tup[0] for tup in ids]
		ids = set(ids)

		self.logger.info('read {} ratings into spark'\
		.upper().format(self.df.count()))

		self.movieDict = MovieDict(self.spark, ids, self.logger)
		self.cachefile = os.path.join(self.data_dir, 'cachedmovies.csv')
		self.cachedmoviesdf = pd.read_csv(self.cachefile).fillna('')
		self.cachedmovieslst = self.cachedmoviesdf.movie.tolist()

	def add_rating(self, movie, rating, userratings, ratings):
		userratings[movie] = round(float(rating),1)
		# local dict
		movie = movie.split(' - ')[0]
		movieId = self.movieDict.lookupmovieids({movie})[0]
		today = dt.datetime.today().strftime("%Y-%m-%d")
		row = (rating, today, movieId)
		ratings.append(row)
		return userratings, ratings

	def rateDate(self,s):
		elems = s.split('-')
		Y = int(elems[0])
		M = int(elems[1])
		D = int(elems[2])
		return Y, M, D

	def loadInputs(self, ratings, userratings):
		self.userId = self.df.agg({'user': 'max'}).collect()[0][0] + 1
		self.logger.info('user {} created'.upper().format(self.userId))
		self.new_ratings = [(self.userId, int(tup[0]),\
		dt.date(*self.rateDate(tup[1])), int(tup[2])) for tup in ratings]

		self.userdf = self.spark.createDataFrame(self.new_ratings,\
		self.df.columns)
		self.logger.info('user has given these ratings: %s' %userratings)
		# self.userratings = {}
		# self.ratings = []

	def make_recommendations(self, n, ratings, userratings):
		self.loadInputs(ratings, userratings)
		self.vldr = Validator(self.df, self.userdf,self.logger,self.dcut)

		als = ALS(rank=8,
				  seed=42,
				  maxIter=10,
				  regParam=0.1)

		#see if this is neccesary, was this just to set schema?
		self.train_df=self.spark.createDataFrame(self.vldr.train_df.rdd,\
		self.schema)

		self.model = als.fit(self.train_df)
		item_ids = self.predictForUser()
		movies = self.movieDict.lookupmovietitles(set(item_ids))
		topn = []
		for mvi in movies:
			if len(topn) == n:
				break
			if mvi in userratings:
				continue
			tl, ov, pp, mp = self.hitmoviedb(mvi)
			if tl or ov or pp:
				topn.append((mvi, tl, ov, pp, mp))
		self.logger.info('%s was recommended : %s'%(self.userId, topn))
		return topn

	def predictForUser(self):
		userCol = self.spark.createDataFrame([(self.userId,)], ['user'])
		movieRecs = self.model.recommendForUserSubset(userCol, 400)
		self.logger.info('making recommendations for {}'\
		.upper().format(self.userId))
		item_ids = []
		recs = movieRecs.select('recommendations').collect()
		for tup in recs[0]['recommendations']:
			item_ids.append(tup['item'])
		return item_ids

	def updateCache(self, mv, tl, ov, pp, mp):
		toappend = {'movie': mv,
		'tagline':tl,
		'overview':ov,
		'posterpath':pp,
		'moviepage':mp}
		self.cachedmoviesdf = self.cachedmoviesdf.append(toappend,
		ignore_index=True)
		self.cachedmoviesdf.to_csv(self.cachefile, index=False)
		self.cachedmoviesdf = pd.read_csv(self.cachefile)

	def hitmoviedb(self, movie):
		moviepagebase = 'https://www.themoviedb.org/movie/%s'
		imgbase = 'https://image.tmdb.org/t/p/original%s'
		msplit = movie.split(' - ')
		movieTitle = msplit[0]
		movieYear = None
		if len(msplit) > 1:
			movieYear = msplit[1]
		gsearch = 'https://www.google.com/search?q=%s'
		gparam = movie.replace('- ', '').replace(' ', '+')
		glink = gsearch % gparam
		if movie in self.cachedmovieslst:
			r = self.cachedmoviesdf[self.cachedmoviesdf.movie==movie]
			tl = r['tagline'].iloc[0]
			ov = r['overview'].iloc[0]
			pp = r['posterpath'].iloc[0]
			mp = r['moviepage'].iloc[0]
			if not mp:
				mp = glink
			return tl, ov, pp, mp
		if self.search:
			response = self.search.movie(query=movieTitle)
			mvid = None
			if movieYear:
				for res in response['results']:
					if res['release_date'].split('-')[0] == movieYear:
						mvid = res['id']
						break
			else:
				try:
					mvid = response['results'][0]['id']
				except:
					mvid = None
			if not mvid:
				self.updateCache(movie, '', '', '', '')
				return '', '', '', glink

			mp = moviepagebase % mvid
			movieo = tmdb.Movies(mvid)
			thegoods = movieo.info()
			tl = thegoods['tagline']
			ov = thegoods['overview']
			pp = imgbase % thegoods['poster_path']
			self.updateCache(movie,tl,ov,pp,mp)
			return tl, ov, pp, mp
		self.updateCache(movie, '', '', '', '')
		return '', '', '', glink

	def validate_model(self):
		write_name = dt.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
		wn = write_name+'_crossvald.png'
		write_path = os.path.join(self.wd, '..', 'my_app', 'static', wn)
		self.logger.info('validating model'.upper())
		self.vldr.validate(self.model, write_path)
		wrote_name = os.path.join('static', wn)
		self.logger.info('wrote %s' % wrote_name)
		return wrote_name

if __name__ == '__main__':
	import sys
	os.environ['PYSPARK_PYTHON'] = sys.executable
	sc = ps.SparkContext('local[5]')
	eng = Engine(sc, sample=True)
	movie0, movie1, movie2 = 'The Fifth Element - 1997', 'Kids in the Hall: Brain Candy - 1996', 'Batman - 1989'
	rating0, rating1, rating2 = 4.0, 4.0, 4.0
	ur, r = {}, []
	userratings, ratings = eng.add_rating(movie0, rating0, ur, r)
	userratings, ratings = eng.add_rating(movie1, rating1, ur, r)
	userratings, ratings = eng.add_rating(movie2, rating2, ur, r)
	# recs = eng.make_recommendations(4)
	# eng.validate_model()
