import json
import argparse
import os
import logging

import daemon

from flask import Flask, request, send_from_directory

from file_handler import FileHandler
from db import DataBaseStorage


app = Flask(__name__)


@app.route("/storage", methods=['POST', 'GET', 'DELETE'])
def storage():
    """
    File Storage Request Handler
    :return:
    """
    file_handler = app.config['FILE_HANDLER']
    database = app.config['DATA_BASE']

    response_error_bad_request = app.response_class(status=400,
                                                    mimetype='application/json')

    response_success_created = app.response_class(status=201,
                                                  mimetype='application/json')

    response_error_not_found = app.response_class(status=404,
                                                  mimetype='application/json')

    response_success_deleted = app.response_class(status=200,
                                                  mimetype='text/html')
    if request.method == 'POST':

        if 'file' not in request.files:
            response_error_bad_request.response = [json.dumps({'error': 'error in sending file to server'})]
            return response_error_bad_request

        file = request.files['file']

        if not file.filename:
            response_error_bad_request.response = [json.dumps({'error': 'empty file-name'})]
            return response_error_bad_request

        new_file = file_handler.upload(file)

        if new_file:
            database.insert(new_file, file.filename)
            response_success_created.response = [json.dumps({'hash': new_file})]
            return response_success_created

        response_error_bad_request.response = [json.dumps({'error': 'the file already exists'})]
        return response_error_bad_request

    elif request.method == 'GET':
        hash_file = request.args.get('hash')

        if hash_file:
            path, name = file_handler.download(hash_file)
            if path:
                name_file = database.get_name_file(hash_file)
                return send_from_directory(path, name, as_attachment=True, attachment_filename=name_file)
            else:
                response_error_not_found.response = [json.dumps({'error': 'file does not exist'})]
                return response_error_not_found

        response_error_bad_request.response = [json.dumps({'error': 'query has not hash'})]
        return response_error_bad_request

    else:  # DELETE request
        hash_file = request.args.get('hash')

        if hash_file:
            if file_handler.delete(hash_file):
                database.delete(hash_file)
                response_success_deleted.response = ['Successfully deleted']
                return response_success_deleted
            else:
                response_error_not_found.response = [json.dumps({'error': 'file does not exist'})]
                return response_error_not_found

        response_error_bad_request.response = [json.dumps({'error': 'query has not hash'})]
        return response_error_bad_request


def init_parser():
    """
    Init parser arguments
    :return: parser
    """
    parser = argparse.ArgumentParser(
        description='Parser for file storage')

    parser.add_argument(
        'path_storage',
        type=str,
        metavar="FILE",
        help="path to the folder where files are stored",
    )

    parser.add_argument(
        '-a',
        '--host',
        type=str,
        help="host service",
        default='127.0.0.1'
    )

    parser.add_argument(
        '-p',
        '--port',
        type=int,
        help="port service",
        default='5000'
    )

    return parser


if __name__ == '__main__':
    parser = init_parser()

    args = parser.parse_args()
    app.config['FILE_HANDLER'] = FileHandler(args.path_storage)
    app.config['DATA_BASE'] = DataBaseStorage(args.path_storage, 'storage')
    logging.basicConfig(filename='storage.log', level=logging.DEBUG)

    app.run(host=args.host, port=args.port, debug=True)
    with daemon.DaemonContext():
        app.run(host=args.host, port=args.port, debug=True)
