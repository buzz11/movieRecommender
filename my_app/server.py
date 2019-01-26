import os
import sys
import time
import multiprocessing as mp

import cherrypy
import pyspark as ps
from paste.translogger import TransLogger

from my_app import create_Myapp
os.environ['PYSPARK_PYTHON'] = sys.executable

def run_server(app):
    app_logged = TransLogger(app)
    cherrypy.tree.graft(app_logged, '/')
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 80,
        'server.socket_host': '0.0.0.0',
        'tools.sessions.on': True
        })
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    cores = mp.cpu_count()
    if cores > 1:
        cores = int(cores/2)
    sc = ps.SparkContext('local[%s]' % cores)
    src = os.path.join('..', 'src')
    sc.addFile(os.path.join(src, 'ngn.py'))
    sc.addFile(os.path.join(src, 'vldtr.py'))
    sc.addFile(os.path.join(src, 'mvdct.py'))
    sc.addFile(os.path.join(src, 'myLib.py'))
    sc.addFile('my_app.py')
    sample = sys.argv[-1] == 'demo'
    app = create_Myapp(sc, sample=sample)
    run_server(app)
