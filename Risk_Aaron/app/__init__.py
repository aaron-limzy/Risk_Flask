from flask import Flask

from config import Config

from flask_uploads import IMAGES, configure_uploads

from flask_login import LoginManager, UserMixin, login_required
from flask.helpers import get_root_path

from werkzeug.security import generate_password_hash
import os

import logging
from logging.handlers import SMTPHandler
from logging.handlers import RotatingFileHandler
from logging.config import fileConfig

# logger = logging.getLogger('waitress')
# logger.setLevel(logging.INFO)

#from app.routes import db as main_app_db  # blueprint db
import dash

fileConfig('logging.cfg')

def create_app(config_class=Config):
    server = Flask(__name__)
    #server = Flask("Main", static_folder='static')
    server.config.from_object(config_class)

    register_dashapps(server)

    register_extensions(server)
    register_blueprints(server)

    return server

# bootstrap = Bootstrap(server)
# db = SQLAlchemy(server)
# login = LoginManager(server)

def register_extensions(server):
    from app.extensions import db, login, bootstrap, excel, excel_format
    from flask_login import current_user

    db.init_app(server)
    login.init_app(server)
    login.login_view = 'login.login'

    # If user not logged in, we want to enfore a login, else, We want to tell them they need rights.
    # login.login_message = u"Please log in to access this page." if not current_user.is_authenticated \
    #                             else "Kindly contact Risk Team for rights."

    bootstrap.init_app(server)
    excel.init_excel(server)

    # Configure the image uploading via Flask-Uploads
    configure_uploads(server, excel_format)


    if server.config['VANTAGE_UPLOAD_FOLDER']:
        folders = [a for a in server.config['VANTAGE_UPLOAD_FOLDER'].split("/") if (a != "" and a != ".") ]
        for i, folder in enumerate(folders):
            folder = "/".join(folders[:i + 1])  # WE Want the current one as well.
            if os.path.isdir(folder) == False:
                os.mkdir(folder)


    if not server.debug:
        if server.config['MAIL_SERVER']:
            print("Setting up email when Flask Errors. ")
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


    # Want to log down some things to see what has been happening.
    if not os.path.exists('logs'):
        os.mkdir('logs')
    # Max file size of 10 kb, Max 10 backup files.
    file_handler = RotatingFileHandler('logs/Risk_Application.log', maxBytes=15048576,
                                       backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    file_handler.setLevel(logging.INFO)
    server.logger.addHandler(file_handler)

    server.logger.setLevel(logging.INFO)
    server.logger.info('Microblog startup')

    # Create DB Context for all blueprints?
    # with server.app_context():
    #     db.create_all()

    # with server.app_context():
    #     db.init_db()
    #import yourapplication.models
    #Base.metadata.create_all(bind=engine)

def register_blueprints(server):
    from app.Swaps.routes import swaps
    from app.routes import main_app
    from app.Plotly.routes import analysis
    from app.errors import bp as errors_bp
    from app.Login.routes import login_bp
    from app.Risk_Client_Tools.routes import Risk_Client_Tools_bp


    # # Want to see if we can init the Blueprint db
    # main_app_db.init_app(server)
    #
    #
    # with server.app_context():
    #     main_app_db.create_all()  # <--- Create blueprint db.



    server.register_blueprint(errors_bp)
    server.register_blueprint(login_bp, url_prefix='/User')
    server.register_blueprint(main_app)
    server.register_blueprint(swaps)
    server.register_blueprint(analysis)
    server.register_blueprint(Risk_Client_Tools_bp, url_prefix='/Risk/Tools')


def register_dashapps(app):
    from app.dashapp1.layout import layout
    from app.dashapp1.callbacks import register_callbacks

    # Meta tags for viewport responsiveness
    meta_viewport = {"name": "viewport", "content": "width=device-width, initial-scale=1, shrink-to-fit=no"}

    dashapp1 = dash.Dash(__name__,
                         server=app,
                         url_base_pathname='/dashboard/',
                         assets_folder=get_root_path(__name__) + '/dashboard/assets/',
                         meta_tags=[meta_viewport])

    with app.app_context():
        dashapp1.title = 'Dashapp 1'
        dashapp1.layout = layout
        register_callbacks(dashapp1)

    _protect_dashviews(dashapp1)


def _protect_dashviews(dashapp):
    for view_func in dashapp.server.view_functions:
        if view_func.startswith(dashapp.config.url_base_pathname):
            dashapp.server.view_functions[view_func] = login_required(dashapp.server.view_functions[view_func])



from app import routes



#import pyexcel
#result = db.engine.execute("select * from aaron.aaron_buf")
#