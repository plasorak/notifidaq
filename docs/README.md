# NotifiDAQ
Exploring

## Setup
```bash
docker-compose up
python -m venv venv
source venv/bin/activate
pip install -e .
./generate_protos
```

### Producer
```bash
fake-controller [OPTIONS] INSTANCE_NAME SESSION_NAME
or:
fake-application [OPTIONS] INSTANCE_NAME SESSION_NAME
```

### Consumer
```bash
notifidaq-consumer-all [OPTIONS] INSTANCE_NAME SESSION_NAME
```
