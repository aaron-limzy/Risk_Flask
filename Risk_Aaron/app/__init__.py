from flask import Flask
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from config import Config
from flask_uploads import UploadSet, IMAGES, configure_uploads
import flask_excel as excel
from flask_login import LoginManager, UserMixin, login_required
from werkzeug.security import generate_password_hash
import os


import dash
import dash_html_components as html

import logging
from logging.handlers import SMTPHandler


server = Flask(__name__)

#server = Flask("Main")
server.config.from_object(Config)
bootstrap = Bootstrap(server)
db = SQLAlchemy(server)
excel.init_excel(server)
login = LoginManager(server)
login.login_view = 'login'
# Configure the image uploading via Flask-Uploads
excel_format = UploadSet('files',  extensions=('xls', 'xlsx', 'csv'))
configure_uploads(server, excel_format)

if server.config['VANTAGE_UPLOAD_FOLDER']:
    folders = [a for a in server.config['VANTAGE_UPLOAD_FOLDER'].split("/") if (a != "" and a != ".") ]
    for i, folder in enumerate(folders):
        folder = "/".join(folders[:i + 1])  # WE Want the current one as well.
        if os.path.isdir(folder) == False:
            os.mkdir(folder)

from app import routes, errors


from app.Swaps.routes import swaps

server.register_blueprint(swaps)

if not server.debug:
    if server.config['MAIL_SERVER']:
        auth = None
        if server.config['MAIL_USERNAME'] or server.config['MAIL_PASSWORD']:
            auth = (server.config['MAIL_USERNAME'], server.config['MAIL_PASSWORD'])
        secure = None
        if server.config['MAIL_USE_TLS']:
            secure = ()
        mail_handler = SMTPHandler(
            mailhost=(server.config['MAIL_SERVER'], server.config['MAIL_PORT']),
            fromaddr='no-reply@' + server.config['MAIL_SERVER'],
            toaddrs=server.config['ADMINS'], subject='Risk-Flask Failure.',
            credentials=auth, secure=secure)
        mail_handler.setLevel(logging.ERROR)
        server.logger.addHandler(mail_handler)


app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/dash/'
)

#import pyexcel
#result = db.engine.execute("select * from aaron.aaron_buf")
#