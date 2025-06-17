from kafka import KafkaConsumer

from notifidaq.models.notification_pb2 import Notification, SystemType, NotificationType
from notifidaq.utils import build_topic
import time
import logging


class NotifidaqConsumer:
    def __init__(self, bootstrap:str, instance_name:str=None, system_type:SystemType=None, session_name:str=None):

        self.bootstrap = bootstrap
        self.instance_name = instance_name
        self.system_type = system_type
        self.session_name = session_name
        
        self.log = logging.getLogger("NotifidaqConsumer")

        self.topic = build_topic(system_type, instance_name, session_name)

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers = self.bootstrap,
            value_deserializer = lambda v: Notification.FromString(v),
        )

    def _select_notification(self, notification: Notification, notification_type, instance_name:str=None, session_name:str=None, system_type:SystemType = None) -> bool:
        if instance_name is not None and notification.source.instance_name != instance_name:
            return False
        if session_name is not None and notification.source.session_name != session_name:
            return False
        if system_type is not None and notification.source.system != system_type:
            return False
        return True


    def consume(self, notification_type, instance_name:str=None, session_name:str=None, system_type:SystemType = None):
        for message in self.consumer:
            notification = message.value
            if self._select_notification(notification, notification_type, instance_name, session_name, system_type):
                type_url = notification.payload.type_url
                type_name = type_url.split('.')[-1]
                self.log.info(f"Receive message '{type_name}' on '{self.topic}'")

                yield notification
            

    def await_notification(self, notification_type, timeout:int=0, instance_name:str=None, session_name:str=None, system_type:SystemType = None):
        start_time = time.time()
        for notification in self.consume(notification_type, instance_name, session_name, system_type):
            if timeout > 0 and (time.time() - start_time) > timeout:
                break
            return notification
        return None
