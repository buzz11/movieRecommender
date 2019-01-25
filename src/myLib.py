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
    pngs = glob('../my_app/static/*.png')
    for png in pngs:
        os.remove(png)

    plt.style.use('ggplot')
    plt.figure(figsize=(8,8))
    font = {'weight': 'bold',
            'size': 12}
    plt.rc('font', **font)
    ms = pd.Series(mscores) * 100
    ps = pd.Series(pscores) * 100
    plt.hist([ms, ps],
             bins=10,
             histtype='bar',
             color=['purple', 'gray'])
    plt.ylim(0, 150)
    # plt.axis([0, 100, 0, .25])

    # ps.hist(bins=100,
    #         range=(0,100),
    #         density=True,
    #         # alpha=.7,
    #         color='fuchsia',
    #         label='Recommendations from popularity')

    ps.plot(kind='kde',
            color='gray',
            # alpha=.7,
            linewidth='2',
            label='_nolegend_')

    plt.axvline(100*sum(pscores)/len(pscores),
                color='gray',
                linestyle='dashed',
                linewidth='1.5',
                label='Mean Matches from popularity')

    # ms.hist(bins=100,
    #         range=(0,100),
    #         density=True,
    #         alpha=.7,
    #         color='cyan',
    #         label='Recommendations from ALS')

    ms.plot(kind='kde',
            color='purple',
            alpha=.7,
            linewidth='2',
            label='_nolegend_')

    plt.axvline(100*sum(mscores)/len(mscores),
                color='purple',
                linestyle='dashed',
                linewidth='1.5',
                label='Mean Matches from ALS')

    plt.xlabel('Percent of Good Recommendations per User')
    plt.ylabel('Relative Frequency')
    plt.title('Recommender Performance')
    plt.legend(loc='upper left', shadow=True)
    plt.savefig(write_path, dpi=96)
