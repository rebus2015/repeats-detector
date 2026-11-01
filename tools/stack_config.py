"""Connection settings for local stack checks; values come from process ENV."""
import os
import urllib.parse


def setting(name, default):
    return os.getenv(name) or default


def rabbit_management():
    amqp = urllib.parse.urlsplit(setting('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/'))
    user = setting('RABBITMQ_MANAGEMENT_USER', urllib.parse.unquote(amqp.username or 'guest'))
    password = setting('RABBITMQ_MANAGEMENT_PASSWORD', urllib.parse.unquote(amqp.password or 'guest'))
    vhost = urllib.parse.unquote(amqp.path[1:]) or '/'
    base = setting('RABBITMQ_MANAGEMENT_URL', 'http://localhost:15672').rstrip('/')
    return base, user + ':' + password, urllib.parse.quote(vhost, safe='')
