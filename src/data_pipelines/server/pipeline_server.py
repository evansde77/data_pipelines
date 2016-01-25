#!/usr/bin/env python
"""
pipeline_server

uwsgi spooler application for queueing and executing pipelines


this module uses the uwsgi API so it needs to be run
under uwsgi.
For example:

uwsgi --spooler=/tmp/spooler \
      --master \
      --http-socket 127.0.0.1:3031 \
      -w data_pipelines.server.pipeline_server:APP

"""
import json
import os
import uwsgi
from flask import Flask, request
from werkzeug.exceptions import BadRequest
from flask.ext.restful import Api, Resource, reqparse
from data_pipelines.pipelines import run_pipeline

from uwsgidecorators import spool, spoolforever
from . import get_logger


LOGGER = get_logger()


@spool
def execute_pipeline(arguments):
    LOGGER.info("consume_feed starting {}".format(arguments))
    run_pipeline(arguments['pipeline'])
    LOGGER.info("consume_feed exiting...")


@spoolforever
def execute_pipeline_continuously(arguments):
    LOGGER.info("consume_feed_continuously starting {}".format(arguments))
    run_pipeline(arguments['pipeline'])
    LOGGER.info("consume_feed_continuously exiting...")


def _parse_request():
    if not request.json:
        raise BadRequest("JSON is required")
    if 'pipeline' not in request.json:
        raise BadRequest("pipeline field not found")
    return request.json['pipeline']


class SpoolerAPI(Resource):
    """
    simple REST API that spools tasks in response to POST requests
    and executes them once.

    Example usage:

    Given data.json like:
    {
        "pipeline": { pipeline config }
    }

    The following curl command will run once using the
    execute_pipeline call defined above.

    curl -H Content-Type:application/json \
         -X POST \
         -d@data.json \
         localhost:3031/data_pipelines/run_once

    """
    def post(self):
        LOGGER.info(u"post()")
        args = _parse_request()
        resp = execute_pipeline(pipeline=json.dumps(args))
        LOGGER.info(resp)
        return {"ok": True, 'spooled': resp}, 202


class ContinuousSpoolerAPI(Resource):
    """
    REST API that spools tasks forever in response to POST
    requests, provides a GET verb to list the jobs
    and also a DELETE verb to remove jobs

    Example Usage:

    Given data.json like:
    {
        "pipeline": { pipeline definition }
    }

    The following curl command will run repeatedly using the
    execute_pipeline_continuously call defined above.

    curl -H Content-Type:application/json \
         -X POST \
         -d@data.json \
         localhost:3031/data_pipelines/run_repeatedly

    The list of running jobs in the spooler can be seen via GET
    to the same URL:

    curl localhost:3031/data_pipelines/run_repeatedly
    {
       "jobs": [
           "/private/tmp/spooler/uwsgi_spoolfile_UUID"],
           "ok": true
    }

    And the job ids in the list can be used to stop running jobs:

    Given del.json like:

    {
        "job":"/private/tmp/spooler/uwsgi_spoolfile_UUID"
    }

    curl -X DELETE \
         -H Content-Type:application/json \
         -d@del.json \
         localhost:3031/data_pipelines/run_repeatedly

    """
    def get(self):
        """
        respond to GET with a list of the jobs
        on the spooler
        """
        jobs = uwsgi.spooler_jobs()
        LOGGER.info(uwsgi.opt)
        LOGGER.info(jobs)
        return {'ok': True, 'jobs': jobs}, 200

    def post(self):
        """
        respond to a POST by adding a new job to the spooler
        that will be continuously spooled

        POST data should be JSON that includes a 'feed' argument

        Response includes the uwsgi job id that was spawned
        """
        LOGGER.info(u"post()")
        args = _parse_request()
        resp = execute_pipeline_continuously(pipeline=json.dumps(args))
        LOGGER.info(resp)
        return {"ok": True, 'spooled': resp}, 202

    def delete(self):
        """
        respond to a delete request by attempting to delete the job
        specified by the job parameter in the payload JSON data

        """
        parser = reqparse.RequestParser()
        parser.add_argument(
            'job',
            type=str,
            location='json',
            required=True
        )
        args = parser.parse_args()
        LOGGER.info('args={}'.format(args))
        job = args['job']
        if args['job'] not in uwsgi.spooler_jobs():
            return {'error': 'job doesnt exist', 'job': job}, 404
        taskname = os.path.basename(job)
        spool_file = os.path.join(uwsgi.opt['spooler'], taskname)
        if not os.path.exists(spool_file):
            msg = "spooler file not found for {}".format(job)
            LOGGER.info("{} - {}".format(msg, spool_file))
            return {'error': msg, 'job': job}, 404
        LOGGER.info("removing: {}".format(spool_file))
        os.remove(spool_file)
        return {"ok": True, 'removed': job}, 200


def build_app():
    """
    build a basic flask app containing the API
    """
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(SpoolerAPI, '/data_pipelines/run_once')
    api.add_resource(ContinuousSpoolerAPI, '/data_pipelines/run_repeatedly')
    return app


APP = build_app()
