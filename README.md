# Easy to use IPC library

    This library is still in development process.
    There will be added Pub/Sub entities and data validation in
    serializers with pydantic and dataclass-factory libraries.

    Stream connectors has no acknowledgements at the moment,
    so messages on service restart will be lost
    (they will be lost anyway on restarting side, but with acks loss can be less)

Plug-n-play IPC

### Idea

Contains two main _entities_: `Client` and `Server`, which uses `Connectors` to create `Connections`.

In most cases, there are one server and few clients. After successful connect there is
no difference between them, any side can make requests to other.

### Currently available connectors:

| Connector | Description |
| --- | --- |
| LocalConnector | Uses Named Pipes on Windows and Unix domain sockets on other systems, if available |
| TcpConnector | Uses TCP/IP protocol, has ability to specify SSLContext |
| RmqConnector | Uses RabbitMQ server, has caveats (documented in connector) |

The most stable connector right now is RmqConnector. Other works too,
but lose messages on every disconnect.

### Extras

You can install additional dependencies for various features

`pip install communica[extraname1, extraname2]`
| Extra name | Feature |
| --- | --- |
| orjson | Faster JSON library, recommended with CPython |
| rabbitmq | RmqConnector, which use AMQP server for communication |
