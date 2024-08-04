# Easy to use IPC library

Sometimes there is no need for complicated
blazingly fast connections with incredible throughput
and even more incredible configuration.
You just want made your processes **communicate** with each other. And that's what this library for.

### Idea

There is two main _entities_: `Client` and `Server`,
which uses `Connectors` to create `Connections`.

The only way client and server differs is who initiate a connection. After successful connect there is
no difference between them, any side can make requests to other.

### Entities

Pairs of Client and Server entities.

| Entity | Description |
| --- | --- |
| Simple | These entities have only one handler and two operations: `request` (send message, wait response, return it) and `throw` (send message and notify responder, that response can be dropped). They use `Serializers` to serialize request data. |
| Route | Similar to simple, but have multiple handlers, identified by exact string match. |

### Connectors:

Things which making connections. Connector can be serialized and passed to other processes, e.g. parent process creates connector, run server with it and start childs, which run clients with same connector.

| Connector | Description |
| --- | --- |
| LocalConnector | Uses Named Pipes on Windows and Unix domain sockets on other systems, if available. This is similar to multiprocessing's connection, but LocalConnector doesn't fallback to TCP. |
| TcpConnector | Uses TCP/IP protocol. |
| RmqConnector | Uses AMQP message broker, can be scaled. |

### Extras

You can install additional dependencies for various features

`pip install communica[extraname1, extraname2]`
| Extra name | Feature |
| --- | --- |
| orjson | Faster JSON library, recommended with CPython. |
| rabbitmq | RmqConnector, which use AMQP server for communication. |
