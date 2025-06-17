from notifidaq.consumer import NotifidaqConsumer
from notifidaq.models.notification_pb2 import SystemType
from notifidaq.models.generic_session_app_notification_pb2 import ApplicationAdvertisedConnectivityService

import click
import logging


@click.command()
@click.option("--bootstrap", default="localhost:9092", help="Bootstrap address")
@click.option("--timeout", default=20, help="Timeout in seconds")
@click.argument("instance_name")
@click.argument("session_name")
def main(bootstrap, timeout, instance_name, session_name):
    FORMAT = "%(message)s"
    logging.basicConfig(level="INFO", format=FORMAT, datefmt="[%X]")
    log = logging.getLogger("consume_all")

    consumer = NotifidaqConsumer(
        bootstrap=bootstrap,
        instance_name=instance_name,
        system_type=SystemType.DAQ_APPLICATION,
        session_name=session_name
    )

    notification_type = ApplicationAdvertisedConnectivityService
   
    notification = consumer.await_notification(
        notification_type=notification_type,
        timeout=timeout
    )

    if notification:
        log.info("Received notification")
    else:
        log.info(f"Timeout: No matching notification received in {timeout} seconds.")


if __name__ == "__main__":
    main()