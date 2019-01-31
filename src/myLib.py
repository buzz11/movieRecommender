import os
from glob import glob

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import desc

def getunpopular(df, n, asc=True):
    movies = df.groupBy('item').agg({'user':'count'})
    ordrr = movies['count(user)']
    if not asc:
        return movies.orderBy(ordrr.desc()).limit(n)
    n = int(.1*movies.count())
    return movies.orderBy(ordrr).limit(n)

def comparePop(x):
    userliked = False
    if x[2] > 2:
        userliked = True
    return x[1], int(True==userliked)

def compare_rates(x):
    realrating = x[2]
    modelrating = x[3]
    userliked = False
    if realrating > 2:
        userliked = True
    recd = False #does top 400 recommendations end w/ low predicted r?
    if modelrating >= 2.5:
        recd = True
    return x[0], int(userliked==recd)

def performanceResultsViz(mscores, pscores, write_path):
    pngpaths = os.path.join('..', 'my_app', 'static', '*.png')
    pngs = glob(pngpaths)
    for png in pngs:
        if 'exmple.png' in png:
            continue
        os.remove(png)
    plt.style.use('ggplot')
    plt.figure(figsize=(8,8))
    font = {'weight': 'bold',
            'size': 12}
    plt.rc('font', **font)
    mscores = pd.Series(mscores) * 100
    pscores = pd.Series(pscores) * 100
    plt.figure(figsize=(8,8))
    plt.style.use('seaborn-white')
    plt.hist([mscores, pscores],
        bins=25,
        histtype='bar',
        normed=True,
        color=['steelblue','magenta'])

    plt.axis([-10, 110, 0, .25])

    pscores.plot(kind='kde',
        color='deeppink',
        linewidth='1',
        label='_nolegend_')

    plt.axvline(sum(pscores)/len(pscores),
        color='deeppink',
        linestyle='dashed',
        linewidth='1',
        label='Average Popularity %')

    mscores.plot(kind='kde',
        color='dodgerblue',
        linewidth='1',
        label='_nolegend_')

    plt.axvline(sum(mscores)/len(mscores),
        color='dodgerblue',
        linestyle='dashed',
        linewidth='1',
        label='Average Recommender %')

    plt.xlabel('Good Recommendations %')
    plt.ylabel('Relative Frequency')
    plt.title('Recommender Performance')
    plt.legend(loc='upper left', shadow=True)
    plt.savefig(write_path, dpi=96)
    return mscores, pscores
