from google.protobuf.any_pb2 import Any
from notifidaq.models.notification_pb2 import SystemType





def pack_to_any(data):
    any = Any()
    any.Pack(data)
    return any

def build_topic(system_type:SystemType, instance_name:str, session_name:str = None):
    if session_name:
        return f"notification.{SystemType.Name(system_type).lower()}.{session_name}.{instance_name}"
    else:
        return f"notification.{SystemType.Name(system_type).lower()}.{instance_name}"
