### [Apache Spark Movie Recommender](http://www.rybot.xyz)

This web app can give you some movie recommendations via collaborative filtering.
Try it out by clicking the above heading. Or clone this repo and follow the instructions below to run locally.

- download netflix-prize-data from [kaggle.com/netflix-inc/netflix-prize-data](http://www.kaggle.com/netflix-inc/netflix-prize-data) to the data directory

- install [dependencies](dependencies.sh) and run app/server.py with python3

```
$ ./dependencies.sh
$ cd my_app
$ python3 server.py
```

By default, only the last three months of ratings are read in. This is about right for running a local spark cluster. To change the cutoff, change the keyword argument in the init function of Engine, which is in src/ngn.py

To run with a subset of movie ratings included in this repo:

```
$ python3 server.py demo
```
