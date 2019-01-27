### Apache Spark Movie Recommender

This web app can give you some movie recommendations via collaborative filtering.

- download netflix-prize-data from [kaggle.com/netflix-inc/netflix-prize-data](http://www.kaggle.com/netflix-inc/netflix-prize-data) to the data directory

- install [dependencies](dependencies.sh) and run app/server.py
```
$ ./dependencies.sh
$ cd my_app
$ python3 server.py
```

By default, only the last three months of ratings are read in. Change the cutoff in ngn.Engine, the latest ratings are 2005/12/31

- to run with the subset of movie ratings included in this repo:
```
$ python3 server.py demo
```
