from flask_login import LoginManager
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
import flask_excel as excel
from flask_uploads import UploadSet
from flask_bootstrap import Bootstrap

db = SQLAlchemy()
login = LoginManager()
bootstrap = Bootstrap()
excel_format = UploadSet('files', extensions=('xls', 'xlsx', 'csv'))
