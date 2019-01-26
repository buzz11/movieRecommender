import os
import sys
import json

import pyspark as ps
from wtforms import Form, TextField, SubmitField
from flask import Blueprint, Flask, Response, render_template, request, session

sys.path.append(os.path.join('..', 'src'))
from ngn import Engine

main = Blueprint('main', __name__)

class SearchForm(Form):
	autocomp = TextField('movie', id='movie_autocomplete')
	submit = SubmitField('rate')

@main.route('/_autocomplete', methods=['GET'])
def autocomplete():
	return Response(json.dumps(re.movieDict.options),\
	mimetype='application/json')

@main.route('/', methods=['GET', 'POST'])
def index():
	session['ratings'] = []
	session['userratings'] = {}
	form = SearchForm(request.form)
	return render_template('index.html', form=form)

@main.route('/submitRating', methods=['POST'])
def askForRating():
	form = SearchForm(request.form)
	global movie
	movie = str(request.form['autocomp'])
	if movie not in re.movieDict.movieset:
		if len(session.get('userratings')) == 0:
			return render_template('notinlistnorates.html', form=form)
		return render_template('notinlist.html',
		form=form,
		userratings=session.get('userratings'))

	tagline, overview, posterpath, mp = re.hitmoviedb(movie)
	if not tagline and not overview and not posterpath:
		return\
		render_template('ratingsubmit_nodb.html', movie=movie, mp=mp)
	return render_template('ratingsubmit.html',
	movie=movie, tl=tagline, ov=overview, pp=posterpath, mp=mp)

@main.route('/pickAnother', methods=["POST"])
def addRating():
	form = SearchForm(request.form)
	rating = request.form['rating']
	ur = session.get('userratings')
	r = session.get('ratings')
	session['userratings'], session['ratings'] =\
	re.add_rating(movie, rating, ur, r)
	return render_template('pickmore.html', form=form,
	userratings=session.get('userratings'))

@main.route('/makeRecommendations', methods=["POST"])
def makeRecommendations():
	ur = session.get('userratings')
	r = session.get('ratings')
	result = re.make_recommendations(10, r, ur)
	return render_template('results.html', result=result)

@main.route('/validateModel', methods=["POST"])
def validateModel():
	wrote_name = re.validate_model()
	return render_template('modelval.html', wn=wrote_name)

def create_Myapp(spark_context, sample=False):
	global re
	re = Engine(spark_context, sample=sample)
	app = Flask(__name__)
	app.secret_key = os.urandom(16)
	app.register_blueprint(main)
	return app

if __name__== '__main__':
	import os
	os.environ['PYSPARK_PYTHON'] = sys.executable
	import pyspark as ps
	sc = ps.SparkContext()
	app = create_Myapp(sc)
	host = '0.0.0.0'
	port = '80'
	debug = True
	threaded = True
	app.run(host, port, debug=True, threaded=True)
