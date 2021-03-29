import json

# DFF TODO -- Not critical for W4111, but should switch from print statements to logging framework.
import logging

from datetime import datetime

from flask import Flask, Response
from flask import Flask, request, render_template, jsonify
from flask_cors import CORS

import utils.rest_utils as rest_utils

from Services.FantasyService.FantasyTeam import FantasyTeam as FantasyTeam
from Services.FantasyService.FantasyManager import FantasyManager as FantasyManager
from Services.LahmanService.PersonService import PersonService as PersonService
from Services.LahmanService.TeamService import TeamService as TeamService

from Services.FantasyService.FantasyPlayer import FantasyPlayer as FantasyPlayer
from Services.FantasyService.FantasyLeague import FantasyLeague as FantasyLeague

from Services.DataServices.Neo4JDataTable import HW3Graph as HW3Graph

# DFF TODO - We should not hardcode this here, and we should do in a context/environment service.
# OK for W4111 - This is not a course on microservices and robust programming.
#
#
auth = {
    "user": "admin",
    "password": "orphanage73",
    "host": "database-1.cie2eqwscgmp.us-east-2.rds.amazonaws.com",
    "db": "HW3_s21"
}

_service_factory = {
    "FantasyTeam": FantasyTeam({
        "db_name": "HW3_s21",
        "table_name": "FantasyTeam",
        "db_connect_info": auth,
        "key_columns": ["teamID"]
    }),
    "FantasyManager": FantasyManager({
        "db_name": "HW3_s21",
        "table_name": "FantasyManager",
        "db_connect_info": auth,
        "key_columns": ["managerID"]
    }),
    "FantasyPlayer": FantasyPlayer({
        "db_name": "HW3_s21",
        "table_name": "FantasyPlayer",
        "db_connect_info": auth,
        "key_columns": ["playerID"]
    }),
    "FantasyLeague": FantasyLeague({
        "db_name": "HW3_s21",
        "table_name": "FantasyLeague",
        "db_connect_info": auth,
        "key_columns": ["leagueID"]
    }),
    "person": PersonService({
        "db_name": "HW3_s21",
        "table_name": "people",
        "db_connect_info": auth,
        "key_columns": ["playerID"]
    }),
    "teams": TeamService({
        "db_name": "HW3_s21",
        "table_name": "recent_teams",
        "db_connect_info": auth,
        "key_columns": ["teamID", "yearID"]
    })
}


# Given the "resource"
def _get_service_by_name(s_name):
    result = _service_factory.get(s_name, None)
    return result


app = Flask(__name__)
CORS(app)


##################################################################################################################


# DFF TODO A real service would have more robust health check methods.
@app.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="app/json")
    return rsp


# TODO Remove later. Solely for explanatory purposes.
# The method take any REST request, and produces a response indicating what
# the parameters, headers, etc. are. This is simply for education purposes.
#
@app.route("/api/demo/<parameter1>", methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/api/demo/", methods=["GET", "POST", "PUT", "DELETE"])
def demo(parameter1=None):
    # Mostly for isolation. The rest of the method is isolated from the specifics of Flask.
    inputs = rest_utils.RESTContext(request, {"parameter1": parameter1})

    # DFF TODO -- We should replace with logging.
    r_json = inputs.to_json()
    msg = {
        "/demo received the following inputs": inputs.to_json()
    }
    print("/api/demo/<parameter> received/returned:\n", msg)

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


##################################################################################################################
# Actual routes begin here.
#
#

@app.route("/api/<resource>/count", methods=["GET"])
def get_resource_count(resource):
    """
    Currently not implemented. Need to revise.
    """
    rsp = Response("NOT IMPLEMENTED", status=501)
    return rsp

    """
    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_resource_count", inputs)

        service = _get_service_by_name(resource)

        if service is not None:
            res = service.get_count()
            if res is not None:
                res = {"count": res}
                res = json.dumps(res, default=str)
                rsp = Response(res, status=200, content_type="application/JSON")
            else:
                rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT FOUND", status=404)

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + resource + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp
    """


@app.route("/api/people/<player_id>/career_batting", methods=["GET"])
def get_career_batting(player_id):
    rsp = Response("NOT IMPLEMENTED", status=501)
    return rsp

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_resource_count", inputs)

        service = _get_service_by_name("player_performance")

        if service is not None:
            if inputs.method == "GET":
                res = service.get_career_batting(player_id)
                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            else:
                rsp = Response("NOT IMPLEMENTED", status=501)
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/players/<player_id>/career_batting, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp

# Done
@app.route("/api/<resource>", methods=["GET", "POST"])
def get_resource_by_query(resource):
    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_resource_by_query", inputs)
        template = inputs.args
        if inputs.method == "GET":
            service = _get_service_by_name(resource)
            if service is not None:
                res_rds = service.find_by_template(template, inputs.fields)
                neo_db = HW3Graph()
                tmp = {"label": resource}
                res_neo = neo_db.find_nodes_by_template(tmp)
                if res_rds is not None and res_neo is not None:
                    # consistency = consistency_check(res_rds, res_neo)
                    res = json.dumps({"SQL": res_rds, "Neo4J": res_neo}, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        elif inputs.method == "POST":
            service = _get_service_by_name(resource)
            template = inputs.data
            res = service.find_by_template(template)
            if service is not None and not res:
                res_rds = service.create(inputs.data)
                neo_db = HW3Graph()
                tmp = {**inputs.data}
                res_neo = neo_db.create_node(label=resource, **tmp)
                if res_rds is not None and res_neo is not None:
                    key = "_".join(res_rds.values())
                    headers = {"location": "/api/" + resource + "/" + key}
                    rsp = Response("CREATED", status=201, content_type="text/plain", headers=headers)
                else:
                    rsp = Response("UNPROCESSABLE ENTITY", status=422, content_type="text/plain")
            else:
                rsp = Response("Service is unavailable or a row with duplicate primary key exists"
                               , status=404, content_type="text/plain")
        else:
            rsp = Response("NOT IMPLEMENTED", status=501)
    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/<resource>, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp

# Done
@app.route("/api/<resource>/<resource_id>", methods=["GET", "PUT", "DELETE"])
def resource_by_id(resource, resource_id):

    try:
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("resource_by_id", inputs)
        # The resource_id can map to a single attribute, e.g. SSNO
        # Or map to a composite key, e.g. {countrycode, phoneno}
        # We encode this as "countrycode_phoneno"
        # 1. /teams/BOS_2004/player is converted to select * from appearances where teamID='BOS' and yearID='2004'
        # 2. /teams/BOS_2004/player/damonjo01 is converted to
        #  select * from appearances where teamID='BOS' and yearID='2004' and playerID='damonjo01'
        # 3. /fatasy_team/BOS2121/player is converted to select * from fantasy_player where teamID='BOS2111'
        resource_key_columns = rest_utils.split_key_string(resource_id)

        if inputs.method == "GET":
            service = _get_service_by_name(resource)
            if service is not None:
                # the first part of the path is the “primary key” of the resource relative to the collection.
                res_rds = service.find_by_primary_key(resource_key_columns)
                res_neo = res_rds
                if res_rds:
                    neo_db = HW3Graph()
                    template_pk = list(res_rds.keys())[0]
                    template_graph = {"label": resource, "template": {template_pk: res_rds[template_pk]}}
                    res_neo = neo_db.find_nodes_by_template(template_graph)
                if res_rds is not None and res_neo is not None:
                    res = json.dumps({"SQL": res_rds, "Neo4J": res_neo}, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")

                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        elif inputs.method == "PUT":
            service = _get_service_by_name(resource)
            if service is not None:
                if service.find_by_primary_key(resource_key_columns):
                    service.update(resource_key_columns, inputs.data)
                    rsp = Response("Update", status=200, content_type="text/plain")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            else:
                rsp = Response("NOT IMPLEMENTED", status=501)
        elif inputs.method == "DELETE":
            service = _get_service_by_name(resource)
            res = service.find_by_primary_key(resource_key_columns)
            if service is not None and res:
                template_pk = list(res.keys())[0]
                template_delete = {template_pk: res[template_pk]}
                res = service.delete(template_delete)
                rsp = Response(res, status=200, content_type="text/plain")
            else:
                rsp = Response("NOT FOUND", status=501)
        else:
            rsp = Response("NOT SUPPORTED METHOD", status=501)
    except Exception as e:
        # DFF TODO -- Need to handle integrity exceptions, etc. more clearly, e.g. 422, etc.
        # TODO Put a common handler to catch exceptions, log the error and return correct
        # HTTP status code.
        print("/api/person, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp

# Done
@app.route("/api/<resource1>/<resource1_id>/<resource2>", methods=["GET", "POST"])
def get_2resource_by_query(resource1, resource1_id, resource2):
    try:
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("resource2_by_id", inputs)
        resource_key_columns = rest_utils.split_key_string(resource1_id)
        service1 = _get_service_by_name(resource1)
        service2 = _get_service_by_name(resource2)
        if inputs.method == "GET":
            if service1 is not None:
                res1 = service1.find_by_primary_key(resource_key_columns)
                if service2 is not None and not res1:
                    res = json.dumps([], default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                elif res1 and (resource2 == 'Follows' or resource2 == 'Likes'):
                    neo_db = HW3Graph()
                    follower_template = create_node_template(res1, resource1)
                    follower = neo_db.find_nodes_by_template(follower_template)
                    if follower:
                        res = neo_db.find_by_relationship(follower_template, resource2)
                        res = json.dumps(res, default=str)
                        rsp = Response(res, status=200, content_type="application/JSON")
                    else:
                        rsp = Response("NOT FOUND", status=404, content_type="text/plain")
                elif res1 is not None:
                    template2 = create_node_template(res1, resource1)
                    res = service2.find_by_template(template2['template'])
                    if res is not None:
                        res = json.dumps(res, default=str)
                        rsp = Response(res, status=200, content_type="application/JSON")
                    else:
                        rsp = Response("NOT FOUND", status=404, content_type="text/plain")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            else:
                rsp = Response("SERVICE NOT FOUND", status=404, content_type="text/plain")
        elif inputs.method == "POST":
            if service1 is not None:
                res1 = service1.find_by_primary_key(resource_key_columns)
                if service2 is not None:
                    if not res1:
                        rsp = Response("Violate Foreign Key constraint, resource1_id does not exist in resource1",
                                       status=200, content_type="application/JSON")
                    elif res1 is not None:  # FK constraint passes
                        res_rds = service2.create(inputs.data)
                        neo_db = HW3Graph()
                        tmp = {**inputs.data}
                        res_neo = neo_db.create_node(label=resource2, **tmp)
                        if res_rds is not None and res_neo is not None:
                            key = "_".join(res_rds.values())
                            headers = {"location": "/api/" + resource2 + "/" + key}
                            rsp = Response("CREATED", status=201, content_type="text/plain", headers=headers)
                        else:
                            rsp = Response("UNPROCESSABLE ENTITY", status=422, content_type="text/plain")
                    else:
                        rsp = Response("NOT IMPLEMENTED", status=501)
                elif res1 and (resource2 == 'Follows' or resource2 == 'Likes'):
                    neo_db = HW3Graph()
                    follower_template = create_node_template(res1, resource1)
                    followee_template = inputs.data
                    follower = neo_db.find_nodes_by_template(follower_template)
                    followee = neo_db.find_nodes_by_template(followee_template)
                    if follower and followee:
                        res = neo_db.create_relationship(follower[0], resource2, followee[0])
                        res = json.dumps("CREATE", default=str)
                        rsp = Response(res, status=200, content_type="application/JSON")
                    else:
                        rsp = Response("NOT FOUND", status=404, content_type="text/plain")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            else:
                rsp = Response("SERVICE NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT IMPLEMENTED", status=501)
    except Exception as e:
        # TODO Put a common handler to catch exceptions, log the error and return correct
        # HTTP status code.
        print("/api/<resource>, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


# Done
@app.route("/api/<resource1>/<resource1_id>/<resource2>/<resource2_id>", methods=["GET", "PUT", "DELETE"])
def op_2resource_by_id(resource1, resource1_id, resource2, resource2_id):
    try:
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("resource2_by_id", inputs)
        resource1_key_columns = rest_utils.split_key_string(resource1_id)
        resource2_key_columns = rest_utils.split_key_string(resource2_id)
        service1 = _get_service_by_name(resource1)
        service2 = _get_service_by_name(resource2)
        if service1 is not None and service2 is not None:
            res1 = service1.find_by_primary_key(resource1_key_columns)
            res2 = service2.find_by_primary_key(resource2_key_columns)
            if inputs.method == "GET":
                if not res1 or not res2:
                    res = json.dumps([], default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                elif res1 is not None and res2 is not None:
                    res = json.dumps(res2, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            elif inputs.method == "PUT":
                if not res1 or not res2:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
                elif res1 is not None and res2 is not None:
                    service2.update(resource2_key_columns, inputs.data)
                    rsp = Response("Update", status=200, content_type="text/plain")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            elif inputs.method == "DELETE":
                if not res1 or not res2:
                    rsp = Response("NOT FOUND", status=404)
                elif res1 is not None and res2 is not None:
                    template2_pk = list(res2.keys())[0]
                    template2_delete = {template2_pk: res2[template2_pk], list(res1.keys())[0]: list(res1.values())[0]}
                    res = service2.find_by_template(template2_delete)
                    if res:
                        res = service2.delete(template2_delete)
                        rsp = Response(res, status=200, content_type="text/plain")
                    else:
                        rsp = Response("NOT FOUND", status=404, content_type="text/plain")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            else:
                rsp = Response("Method NOT FOUND", status=501)
        else:
            rsp = Response("Service unavailable", status=501)
    except Exception as e:
        # DFF TODO -- Need to handle integrity exceptions, etc. more clearly, e.g. 422, etc.
        # TODO Put a common handler to catch exceptions, log the error and return correct
        # HTTP status code.
        print("/api/person, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/people/search/<pattern>", methods=["GET"])
def get_person_by_pattern(pattern):
    rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.

        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_person_by_pattern", inputs)

        # resource_key_columns = rest_utils.split_key_string(resource_id)

        if inputs.method == "GET":
            service = _get_service_by_name("people")

            if service is not None:
                res = service.get_by_pattern("nameLast", pattern)
                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT IMPLEMENTED", status=501)
    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/people/pattern, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


def create_node_template(res, resource):
    template = None
    if res:
        template_pk = list(res.keys())[0]
        template2 = {template_pk: res[template_pk]}
        template = {"label": resource, "template": template2}
    return template


if __name__ == '__main__':
    # host, port = ctx.get_host_and_port()

    # DFF TODO We will handle host and SSL certs different in deployments.
    app.run(host="0.0.0.0", port=5001)
