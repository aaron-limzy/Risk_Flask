from app import create_app

from waitress import serve

from logging.config import dictConfig

import logging


def effectivehandlers(logger):
    handlers = logger.handlers
    while True:
        logger = logger.parent
        handlers.extend(logger.handlers)
        if not (logger.parent and logger.propagate):
            break
    return handlers

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
# logger.propagate = False
# print(effectivehandlers(logger))
#
# Need to uncomment the serve for it to work on production level.
# serve(server, host='0.0.0.0', port=5000, threads=12)


#server.run()
#server.app_context().push()


