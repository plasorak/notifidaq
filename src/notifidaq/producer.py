from google.protobuf.message import Message as msg
from google.protobuf.timestamp_pb2 import Timestamp
import logging
from kafka import KafkaProducer

from notifidaq.models.notification_pb2 import SystemType, Notification, Origin
from notifidaq.utils import pack_to_any, build_topic

class NotifidaqProducer:
    def __init__(
        self,
        bootstrap:str,
        instance_name:str,
        system_type:SystemType,
        session_name:str = None,

    ) -> None:

        self.log = logging.getLogger("NotifidaqProducer")

        self.bootstrap = bootstrap
        self.system_type = system_type
        self.instance_name = instance_name
        self.session_name = session_name

        self.topic = build_topic(system_type, instance_name, session_name)

        # Setup the opmon publisher
        self.kafka_producer = KafkaProducer(
            bootstrap_servers = self.bootstrap,
            value_serializer = lambda v: v.SerializeToString(),
            key_serializer = lambda k: str(k).encode('utf-8')
        )

    def notify(self, message:msg):

        t = Timestamp()
        t.GetCurrentTime()

        self.log.info(f"Sending message '{message.DESCRIPTOR.name}' to '{self.topic}'")

        notification = Notification(
        source=Origin(
            system=self.system_type,
            instance_name=self.instance_name,
            session_name=self.session_name
        ),
        timestamp=t,
        payload=pack_to_any(message),
    )

        type_name = message.DESCRIPTOR.name
        if type_name == "ModulesInitialised":
            from notifidaq.models.notification_pb2 import AppNotificationType
            notification.app_type = AppNotificationType.Modules_Initialised
        elif type_name == "ControllerFoundChildren":
            from notifidaq.models.notification_pb2 import ControllerNotificationType
            notification.controller_type = ControllerNotificationType.Controller_FoundChildren
        else:
            self.log.warning(f"Unrecognized message type: {type_name}")

        self.log.info(f"Sending message '{type_name}' to '{self.topic}'")

        return self.kafka_producer.send(self.topic, value=notification)

