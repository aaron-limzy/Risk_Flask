from app.routes import main_app
from flask import render_template



from flask import Blueprint

bp = Blueprint('errors', __name__)

from app.errors import handlers


@main_app.errorhandler(404)

def not_found_error(error):
    return render_template('error_template.html', header="Where are you going?", description="Page does not exists.", title="404 Error"), 404


@main_app.errorhandler(500)

def not_found_error(error):
    return render_template('error_template.html', header="An Error has Occured.",description="An unexpected error has occurred.", title="500 Error"), 500
