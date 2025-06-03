from kafka import KafkaConsumer

from notifidaq.models.notification_pb2 import Notification, SystemType
from notifidaq.utils import build_topic

class NotifidaqConsumer:
    def __init__(self, bootstrap:str, instance_name:str=None, system_type:SystemType=None, session_name:str=None):

        self.bootstrap = bootstrap
        self.instance_name = instance_name
        self.system_type = system_type
        self.session_name = session_name

        self.topic = build_topic(system_type, instance_name, session_name)

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers = self.bootstrap,
            value_deserializer = lambda v: Notification.FromString(v),
        )

    def consume(self):
        for message in self.consumer:
            yield message.value

    def await_notification(self, notification_type:Notification):
        pass
