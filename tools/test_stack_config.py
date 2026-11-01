import os
import unittest
from unittest.mock import patch

from stack_config import rabbit_management


class RabbitManagementSettingsTest(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_local_defaults(self):
        self.assertEqual(rabbit_management(), ('http://localhost:15672', 'guest:guest', '%2F'))

    @patch.dict(os.environ, {
        'RABBITMQ_URL': 'amqps://user%40name:p%3Ass@broker:5671/team%2Fjobs',
        'RABBITMQ_MANAGEMENT_URL': 'https://management.example/',
    }, clear=True)
    def test_encoded_credentials_and_vhost(self):
        self.assertEqual(rabbit_management(), ('https://management.example', 'user@name:p:ss', 'team%2Fjobs'))

    @patch.dict(os.environ, {
        'RABBITMQ_URL': 'amqp://app:password@broker/',
        'RABBITMQ_MANAGEMENT_USER': 'monitor',
        'RABBITMQ_MANAGEMENT_PASSWORD': 'monitor-password',
    }, clear=True)
    def test_management_credentials_override_amqp(self):
        self.assertEqual(rabbit_management()[1], 'monitor:monitor-password')


if __name__ == '__main__':
    unittest.main()
