## Inter-Process Communication (IPC) for Python

Sometimes there is no need for complicated,
blazingly fast frameworks with incredible throughput
and even more incredible configuration.
You just want to make your processes **communicate** with each other.
And that's what this library is for.

### Install

`pip install communica`

You can install additional dependencies for various features

`pip install communica[extraname1, extraname2]`
| Extra name | Feature |
| --- | --- |
| orjson | Faster JSON library, which makes available `OrjsonSerializer` from `communica.serializers.json` module. |
| adaptix | Makes available `communica.serializers.AdaptixSerializer`, which provides request and response data validation. |
| rabbitmq | Makes available `communica.connectors.RmqConnector`, to use AMQP server for communication. |

### Usage

_For clarity, the only difference between *Clients and *Servers
is who connected to who (Clients connect to Server).
Beyond this they are both almost identical. So i will use
termins like "requesting side" and "responding side"._

First, you should pick an *Entity*. This will be the primary object
for interaction with the library, you can think of it as
"Client" or "App" object. Currently, only request-reply based
entities are available. For instance, Simple entities (SimpleServer
and SimpleClient) enable you to send messages and receive responses
(provided the other side defines corresponding handler).

Then, choose how Entities will connect to each other. This is what
*Connectors* for -- they used to establish connections. No matter
which technology Connector use, Entities don't know anything about
it, so you can swap connectors without rewriting much code.
Connectors are serializable to ASCII string,
refer to their .dump_state() and .from_state() methods for details,
as the method parameters are specific to each connector type.

Finally, data in request should somehow be transformed from Python
objects to bytes and back. This is called serialization and
that thing is done by *Serializers*. Currently, there are two serializers:
`JsonSerializer`, which passes objects directly to `json.dump` and
`AdaptixSerializer`, which first converts objects using
[adaptix](https://github.com/reagento/adaptix) library before JSON
serialization. AdaptixSerializer is particularly useful for handling
Python objects with complicated structure.

**Putting it all together**

Server:
```python
import asyncio
from communica import SimpleServer, TcpConnector

connector = TcpConnector('localhost', 16161)

def handler(data: str):
    print(f'Received {data!r} from client')
    return f'Thanks for your {data!r}!'

server = SimpleServer(
    connector=connector,
    handler=handler
)

asyncio.run(server.run())
```
Client:
```python
import asyncio
from communica import SimpleClient, TcpConnector

connector = TcpConnector('localhost', 16161)

async def main():
    async with SimpleClient(connector=connector) as client:
        resp = await client.request('hello')
        print(f'Server responded with {resp!r}')

asyncio.run(main())
```

As you can see, server was started using Entity.run() and
client with Entity's context manager. These methods are available for
both Client and Server.\
Also there is an [example](https://github.com/Elchinchel/communica/blob/master/examples/route_client.py)
for Route entities.

**Entities:**\
Pairs of Client and Server entities.

| Entity | Description |
| --- | --- |
| SimpleClient, SimpleServer | These entities have only one handler and two operations: `request` (send message, wait response, return it) and `throw` (schedule message and don't wait for response). "Throwing" is useful in cases such as logging events or *fire-and-forget* notifications |
| RouteClient, RouteServer | Similar to simple, but have multiple handlers, identified by exact string match. |

**Connectors:**\
Things which making connections.

| Connector | Description |
| --- | --- |
| LocalConnector | Uses Named Pipes on Windows and Unix domain sockets on other systems, if available. This is similar to multiprocessing's connection, but LocalConnector doesn't fallback to TCP (multiprocessing does). |
| TcpConnector | Uses TCP/IP protocol. |
| RmqConnector | Uses AMQP message broker, based on [aiormq](https://github.com/mosquito/aiormq) library. |
