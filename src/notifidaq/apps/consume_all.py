from notifidaq.consumer import NotifidaqConsumer
from notifidaq.models.notification_pb2 import SystemType
import click


@click.command()
@click.option("--bootstrap", default="localhost:9092", help="Bootstrap address")
@click.argument("instance_name")
@click.argument("session_name")
def main(bootstrap, instance_name, session_name):
    consumer = NotifidaqConsumer(
        bootstrap = bootstrap,
        instance_name = instance_name,
        system_type=SystemType.DAQ_APPLICATION,
        session_name = session_name
    )

    print("Subscribed. Waiting for messages...")
    for message in consumer.consume():
        print(message)  # message is a parsed `Notification` protobuf

if __name__ == "__main__":
    main()

