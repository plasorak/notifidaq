# NotifiDAQ
Exploring

## Setup
### Producer
```bash
docker-compose up -d
python -m venv venv
source venv/bin/activate
pip install -e .
./generate_protos
fake-application [OPTIONS] INSTANCE_NAME SESSION_NAME
```

### Consumer
```bash
notifidaq-consumer-all [OPTIONS] INSTANCE_NAME SESSION_NAME
```
