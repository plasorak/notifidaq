from notifidaq.producer import NotifidaqProducer
from notifidaq.consumer import NotifidaqConsumer
from notifidaq.models.notification_pb2 import SystemType
from notifidaq.models.generic_session_app_notification_pb2 import ApplicationInitialised, ApplicationStarted, ApplicationAdvertisedConnectivityService

import click

@click.command()
@click.option("--bootstrap", default="localhost:9092", help="Bootstrap address")
@click.argument("instance_name")
@click.argument("session_name")
@click.argument("children_name", default=[])
def main(bootstrap, instance_name, session_name, children_name):

    producer = NotifidaqProducer(
        bootstrap = bootstrap,
        instance_name = instance_name,
        system_type = SystemType.CONTROLLER,
        session_name = session_name
    )

    producer.notify(ApplicationInitialised())

    consumers = {}
    for child in children_name:
        consumers[child] = NotifidaqConsumer(
            bootstrap = bootstrap,
            instance_name = child,
            system_type = SystemType.DAQ_APPLICATION,
            session_name = session_name
        )

    producer.notify(
        ApplicationStarted()
    )
    producer.notify(
        ApplicationAdvertisedConnectivityService()
    )

if __name__ == "__main__":
    main()
