from flask import render_template
#from app import db
from app.errors import bp


@bp.app_errorhandler(404)
def not_found_error(error):
    return render_template('error_template.html', header="Where are you going?", description="Page does not exists.", title="404 Error"), 404


@bp.app_errorhandler(500)
def not_found_error(error):
    return render_template('error_template.html', header="An Error has Occured.",description="An unexpected error has occurred.", title="500 Error"), 500

