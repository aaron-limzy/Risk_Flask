from app import create_app

from waitress import serve

from logging.config import dictConfig

import logging

from flask.logging import default_handler

# dictConfig({
#     'version': 1,
#     'formatters': {'default': {
#         'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
#     }},
#     'handlers': {'wsgi': {
#         'class': 'logging.StreamHandler',
#         'stream': 'ext://flask.logging.wsgi_errors_stream',
#         'formatter': 'default'
#     }},
#     'root': {
#         'level': 'INFO',
#         'handlers': ['wsgi']
#     }
# })

server = create_app()
# logger = logging.getLogger('waitress')
# logger.setLevel(logging.DEBUG)

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.INFO)

serve(server, host='0.0.0.0', port=5000, threads=8)


#server.run()
#server.app_context().push()