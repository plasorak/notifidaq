from notifidaq.producer import NotifidaqProducer
from notifidaq.consumer import NotifidaqConsumer
from notifidaq.models.notification_pb2 import SystemType
from notifidaq.models.generic_session_app_notification_pb2 import ApplicationInitialised, ApplicationStarted, ApplicationAdvertisedConnectivityService, ApplicationUnregisteredFromConnectivityService, Shutdown
from notifidaq.models.app_notification_pb2 import ModulesInitialised

import logging
import click
import time
import random
from rich.logging import RichHandler

max_sleep = 2 # my seconds are expensive...
app_live_time = 10

@click.command()
@click.option("--bootstrap", default="localhost:9092", help="Bootstrap address")
@click.argument("instance_name")
@click.argument("session_name")
def main(bootstrap, instance_name, session_name):
    FORMAT = "%(message)s"
    logging.basicConfig(level="INFO", format=FORMAT, datefmt="[%X]")
    log = logging.getLogger("fake_app")

    log.info(f"Starting fake application {instance_name} in session {session_name}")
    producer = NotifidaqProducer(
        bootstrap = bootstrap,
        instance_name = instance_name,
        system_type = SystemType.DAQ_APPLICATION,
        session_name = session_name
    )

    time.sleep(random.random() * max_sleep)
    producer.notify(ApplicationInitialised())

    time.sleep(random.random() * max_sleep)
    producer.notify(ApplicationStarted())

    time.sleep(random.random() * max_sleep)
    producer.notify(ModulesInitialised())

    time.sleep(random.random() * max_sleep)
    producer.notify(ApplicationAdvertisedConnectivityService())

    time.sleep(app_live_time)
    producer.notify(ApplicationUnregisteredFromConnectivityService())

    time.sleep(random.random() * max_sleep)
    producer.notify(Shutdown())

    log.info(f"Fake application {instance_name} in session {session_name} shutdown")

if __name__ == "__main__":
    main()
