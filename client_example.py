#!/usr/bin/env python
import pika
import uuid
import json
import config

#Example CRUD Requests
create_request = {
	'method': 'create',
	'resource': 'user',
	'columns': {
		'username': 'jimbo',
		'password': 'passw',
		'email': 'a@a.com'
	}
}

read_request = {
	'method': 'read',
	'resource': 'user',
	'filters': [
		['username', '==', 'jimbo']
	]
}

delete_request = {
	'method': 'delete',
	'resource': 'user',
	'filters': [
		['username', '==', 'jimbo']
	]
}

update_request = {
	'method': 'update',
	'resource': 'user',
	'filters': [
		['username', '==', 'bob']
	],
	'values': {
		'username': 'joe'
	}
}

class ClientExample(object):
	def __init__(self):
		self.rabbitmq_ip = '192.168.56.102'
		self.credentials = pika.PlainCredentials('dg94', 'it490')
		self.connection = pika.BlockingConnection(
			pika.ConnectionParameters(self.rabbitmq_ip, 5672, '/', self.credentials))
		self.channel = self.connection.channel()

		result = self.channel.queue_declare(exclusive=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(self.on_response, no_ack=True,
									queue = self.callback_queue)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self, username):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(	exchange='',
									routing_key='db',
									properties=pika.BasicProperties(
										reply_to = self.callback_queue,
										correlation_id = self.corr_id,
                                        content_type = 'application/json'
										),
									body=username)
		while self.response is None:
			self.connection.process_data_events()
		return self.response

test_client = ClientExample()

print(" [x] Requesting...")
response = test_client.call(json.dumps(update_request))
print(f" [.] Got {response}")
