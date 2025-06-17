from google.protobuf.any_pb2 import Any
from notifidaq.models.notification_pb2 import SystemType, Notification
from google.protobuf.message import Message
import inspect


def pack_to_any(data):
    any = Any()
    any.Pack(data)
    return any

def build_topic(system_type:SystemType, instance_name:str, session_name:str = None):
    if session_name:
        return f"notification.{SystemType.Name(system_type).lower()}.{session_name}.{instance_name}"
    else:
        return f"notification.{SystemType.Name(system_type).lower()}.{instance_name}"
    
def unpack_payload(payload: Any, msg_module, notification_type=None):
        for name, cls in inspect.getmembers(msg_module, inspect.isclass):
            if issubclass(cls, Message) and hasattr(cls, 'DESCRIPTOR'):
                if payload.Is(cls.DESCRIPTOR):
                    msg = cls()
                    payload.Unpack(msg)
                    if notification_type is None or isinstance(msg, notification_type):
                        return msg
                    
        return None

def matches_source(notification: Notification, instance_name=None, session_name=None, system_type=None) -> bool:
        if instance_name and notification.source.instance_name != instance_name:
            return False
        if session_name and notification.source.session_name != session_name:
            return False
        if system_type and notification.source.system != system_type:
            return False
        return True