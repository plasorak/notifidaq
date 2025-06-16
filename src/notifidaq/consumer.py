from kafka import KafkaConsumer
from google.protobuf.any_pb2 import Any
from notifidaq.models import generic_session_app_notification_pb2 as msg_module

from notifidaq.models.notification_pb2 import Notification, SystemType
from notifidaq.utils import build_topic, unpack_payload, matches_source
import logging
import time



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


    def consume(self, notification_type=None, instance_name=None, session_name=None, system_type=None):
        for message in self.consumer:
            notification = message.value
            if not matches_source(notification, instance_name, session_name, system_type):
                continue

            unpacked = unpack_payload(notification.payload, msg_module, notification_type)
            if unpacked:
                self.log.info(f"Received {unpacked.DESCRIPTOR.name} from topic '{self.topic}'")
                yield unpacked

    def await_notification(self, notification_type=None, timeout=20, instance_name=None, session_name=None, system_type=None):
        start_time = time.time()
        for unpacked in self.consume(notification_type, instance_name, session_name, system_type):
            if timeout > 0 and (time.time() - start_time) > timeout:
                break
            return unpacked
        return None