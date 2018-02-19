from datetime import datetime, timedelta
from flask import Blueprint, jsonify, url_for
from earlauto import db
from earlauto.models import Visitor, AppendedVisitor
from earlauto.tasks import append_visitor
home = Blueprint('home', __name__)


@home.before_app_first_request
def init_db():
    db.create_all()
    db.session.commit()


@home.route('/status/<task_id>/', methods=['GET', 'POST'])
def taskstatus(task_id):
    task = append_visitor.AsyncResult(task_id)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'state': task.state,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


@home.route('/visitors/')
def visitors():
    visitors = Visitor.query.all()
    return jsonify([visitor.ip for visitor in visitors])
