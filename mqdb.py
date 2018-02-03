#!/usr/bin/env python
import pika
from config import MqConfig
from sorcery import Sorcery
import sys
import logging
import json
from json import JSONDecodeError
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(filename='mqdb.log',level=logging.INFO, format='%(asctime)s %(message)s')

#CRUD operation types
CREATE = 'create'
READ   = 'read'
UPDATE = 'update'
DELETE = 'delete'

#Query filters

class Pikachu(object):

    '''
    Middleware that subscribes to RabbitMQ RPC channel via pika and translates
    JSON request messages into database CRUD operations via SQLAlchemy.

    Example request format:

    {
        method: 'create',
        resource: 'user',
        columns: [
            {name: 'username', value: 'test_user'}
        ]
    }
    '''

    def __init__(self):
        self.username     = MqConfig.username
        self.password     = MqConfig.password
        self.host         = MqConfig.host
        self.port         = MqConfig.port
        self.virtual_host = MqConfig.virtual_host
        self.queue        = MqConfig.queue

        self.session = Sorcery.get_session()
        self.logger = logging.getLogger('mqdb')
        self.logger.addHandler(logging.StreamHandler())

    def get_connection(self):
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            return pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port, self.virtual_host, credentials))
        except: #TODO: Catch specific errors
            self.logger.critical(sys.exc_info())
            return

    def get_channel(self):
        connection = self.get_connection()
        if connection:
            channel = connection.channel()
            channel.queue_declare(queue=self.queue)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(self.on_request, queue=self.queue)
            return channel
        else:
            return

    def listen(self):
        channel = self.get_channel()
        if channel:
            self.logger.info(f'\tAwaiting requests on {self.host}:{self.port}{self.virtual_host}')
            channel.start_consuming()

    def on_request(self, ch, method, props, body):
        try:
            request = json.loads(body)
            response = self.fill_request(request)
        except JSONDecodeError as e:
            response = {'error': f'{type(e).__name__}: {str(e)}'}
        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id = props.correlation_id),
            body=json.dumps(response)
        )
        self.logger.info(f'\t\tReceived: {request}\n\t\tReturned: {response}')
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def fill_request(self, request):
        request_method = request.get('method').lower()
        req_resource = request.get('resource').lower()
        Resource = Sorcery.get_resource(req_resource)

        if not Resource:
            return {'error': f"The specified resource <{req_resource}> doesn't exist"}
        try:
            if request_type == CREATE:
                return self.create(request, Resource)
            elif request_method == READ:
                rows = self.read(request, Resource)
                rows = [row.to_dict() for row in rows]
                return {'success': True, 'result': rows}
            elif request_method == UPDATE:
                return self.update(request, Resource)
            elif request_method == DELETE:
                return self.delete(request, Resource)
            else:
                return {'error': "Invalid request type. Valid types are create, read, update, delete. e.g {type: 'create'}"}
        except SQLAlchemyError as e:
            return {'success': False, 'result': str(e)}

    def create(self, request, Resource):
        columns = request.get('columns')
        try:
            for key, value in columns.items():
                resource[key] = value
        except AttributeError as e:
            return {'success': False, 'message': str(e)}

        try:
            self.session.add(resource)
            self.session.commit()
            return {'success': True, request.get('resource').lower(): resource.to_dict()}
        except SQLAlchemyError as e:
            self.session.rollback()
            return {'success': False, 'message': str(e)}

    def read(self, request, Resource):
        filters = request.get('filters')
        rows = self.session.query(Resource)

        for flt in filters :
            col = getattr(Resource, flt[0])
            op = flt[1]
            comp = flt[2]
            if op == '==':
                rows = rows.filter(col==comp)
            elif op == '!=':
                rows = rows.filter(col!=comp)
            elif op == 'like':
                rows = rows.filter(col.like(comp))
            elif op == 'in':
                rows = rows.filter(~col.in_(comp))
            #will add more operators if necessary
        return rows

    def update(self, request, Resource):
        rows = self.read(request, Resource)
        num_rows = len(list(rows))
        for row in rows:
            for key, val in request.get('values').items():
                row[key] = val
        self.session.commit()
        return {'success': True, 'updated': num_rows}

    def delete(self, request, Resource):
        rows = self.read(request, Resource)
        num_rows = len(list(rows))
        for row in rows:
            self.session.delete(row)
        self.session.commit()
        return {'success': True, 'deleted': num_rows}



if __name__ == '__main__':
    pikachu = Pikachu()
    pikachu.listen()
