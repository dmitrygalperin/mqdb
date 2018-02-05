#!/usr/bin/env python
import pika
from config import MqConfig
from dbcon import Dbcon
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


class Mqdb(object):

    '''
    Middleware that subscribes to RabbitMQ RPC channel via pika and translates
    JSON request messages into database CRUD operations via SQLAlchemy.

    Example request format:

    {
        'method': 'update',
    	'resource': 'user',
    	'where': [
    		['username', '==', 'jimbo']
    	],
    	'values': {
    		'username': 'newusername'
    	}
    }
    '''

    def __init__(self):
        self.username     = MqConfig.username
        self.password     = MqConfig.password
        self.host         = MqConfig.host
        self.port         = MqConfig.port
        self.virtual_host = MqConfig.virtual_host
        self.queue        = MqConfig.queue

        self.session = Dbcon.get_session()
        self.logger = logging.getLogger('mqdb')
        self.logger.addHandler(logging.StreamHandler())

    def get_connection(self):
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            return pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port, self.virtual_host, credentials))
        except Exception as e:
            self.logger.critical(e)
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
            self.logger.info('Awaiting requests on {}:{}{}'.format(self.host, self.port, self.virtual_host))
            channel.start_consuming()

    def on_request(self, ch, method, props, body):
        try:
            request = json.loads(body)
            response = self.fill_request(request)
        except JSONDecodeError as e:
            response = {'error': '{}: {}'.format(type(e).__name__, str(e))}
        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id = props.correlation_id),
            body=json.dumps(response)
        )
        self.logger.info('\t\tReceived: {}\n\t\tReturned: {}'.format(request, response))
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def fill_request(self, request):
        try:
            req_method = request.get('method').lower()
            req_resource = request.get('resource').lower()
            vald = request.get('values')
            where_list = request.get('where')
            order_by = request.get('orderBy')
            Resource = Dbcon.get_resource(req_resource)

            if not Resource:
                return {'message': "The specified resource <{}> doesn't exist".format(req_resource)}
            tbl = Resource.__table__

            if req_method == CREATE:
                stmt = tbl.insert()
            elif req_method == READ:
                stmt = tbl.select()
            elif req_method == UPDATE:
                stmt = tbl.update()
            elif req_method == DELETE:
                stmt = tbl.delete()
            else:
                return {'message': "Invalid request type. Valid types are create, read, update, delete. e.g {method: 'create'}"}

                if where_list:
                    stmt = self.set_where(where_list, stmt, tbl)
                if vald:
                    stmt = stmt.values(**vald)
                if order_by:
                    stmt = self.set_order_by(order_by, stmt, tbl)
        except Exception as e:
            self.logger.info(str(e))
            return {'message': str(e)}

        return self.execute(stmt)

    def execute(self, stmt):
        self.logger.info(stmt)
        try:
            res = self.session.execute(stmt)
            self.session.commit()
            try:
                return {'rows': [dict(row) for row in res]}
            except:
                return {'affected': res.rowcount}
        except Exception as e:
            self.logger.info(e)
            self.session.rollback()
            return {'message': str(e)}

    def set_where(self, where_list, stmt, tbl):
        for where in where_list :
            col = getattr(tbl.c, where[0])
            op = where[1]
            comp = where[2]
            if op == '==':
                stmt = stmt.where(col==comp)
            elif op == '!=':
                stmt = stmt.where(col!=comp)
            elif op == 'like':
                stmt = stmt.where(col.like(comp))
            elif op == 'in':
                stmt = stmt.where(~col.in_(comp))
        return stmt

    def set_order_by(self, order_by, stmt, tbl):
        colname, direction = tuple(order_by)
        self.logger.info(colname,direction)
        col = getattr(tbl.c, colname)
        order_func = col.desc() if direction == 'desc' else col.asc()
        return stmt.order_by(order_func)


if __name__ == '__main__':
    mqdb = Mqdb()
    mqdb.listen()
