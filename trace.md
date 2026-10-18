# Implement Connection Pinning in aiohttp

> **Workspace:** d:\document\swe\aiohttp

---

**User**

aiohttp 的客户端连接池按主机、端口、代理和 SSL 组成连接键来复用连接，但调用方没法声明接下来这一段必须回到刚才那条连接。现在只能把连接数收成 1，或者给每个请求单独做一个连接器。请给 ClientSession 增加一种由调用方显式打开的连接钉住范围：范围内、同一个连接键上的后续请求，必须回到打开范围时实际使用的那条连接。

没有打开这种范围时，连接池限额、保活、指纹校验、代理、SSL、Cookie、超时、重定向、WebSocket 和现有测试必须和现在完全一样。

范围只钉住打开时那次请求真正拿到的连接，以及这条连接对应的连接键。别的主机、端口、代理或 SSL 设置仍然走现在的连接池，不能把钉住的连接拿去用。同一个范围里，前一个响应还没有读完或释放时，后一个同键请求要等这条连接被释放后再用它，不能新开第二条。等待适用这次请求自己的超时。超时或调用方取消时，连接限额要按现在的规则恢复，不能把名额占死。

钉住的连接如果在范围结束前断开，范围内之后的同键请求要失败，错误里要能看出是这个范围的连接已经失效，不能悄悄新拨一条来顶替。重定向如果还是同一个连接键，必须留在这条连接上。重定向到别的连接键时，按现在的连接池处理，并且不能把钉住目标改成新主机。WebSocket 升级把连接从池里拿走之后，这个范围就算用尽，后面的请求按连接已失效失败，而不是再拨一条。

两层范围叠在一起时，内层只影响内层，关掉内层后外层仍然钉着原来的连接。同一个会话上两个并发范围不能串连接。关闭会话时，范围和连接按现在的关闭规则收掉。连接获取、复用和排队的追踪回调仍然要触发。

把连接器的总限额或单主机限额改成 1、给每个请求换一个新的连接器、或者在请求外面自己缓存响应对象，都不能算实现了这个功能。钉住必须发生在连接器获取和释放连接的路径上。

**Assistant**

I'll start by exploring the codebase to understand the connection pooling architecture, then design the pinning feature.

Let me explore the connector's core classes and connection acquisition/release paths.


Now let me examine the client session's request flow, redirect handling, and connection lifecycle.


Now let me examine the request/response classes to understand `ConnectionKey`, response connection handling, and release.


Now let me examine the WebSocket connection path to understand how it detaches the connection from the pool.


Let me examine the exception hierarchy, request class slots, exports, and tracing callbacks.


Now let me look at client.py's imports, close logic, and request public entry points.


Let me check all callers/overriders of `connector.connect` and look at existing connector tests for patterns.


Let me look at test helpers in test_connector.py and the functional test patterns.


Let me check functional test patterns for connection counting and websocket tests.


Let me check the changelog fragment types and the client reference docs structure.


I now have a thorough understanding of the architecture. Let me check the WebSocket functional test fixtures before implementing.


I now have a complete understanding of the architecture. Let me implement the feature, starting with the new exception type.


Now the connector. First update imports and exports:


Now add the `ConnectionPin` class before `BaseConnector`:


I need to fix the `Token` type import reference in the new class:


The previews don't modify the file until accepted, so I'll add the `contextvars` module import the new class needs, matching the original file:


Now add pin storage to `BaseConnector.__init__`:


Now refactor `connect` into a pin-aware dispatcher plus the existing path renamed to `_connect_unpinned`, and add pin factory helpers:


Now route releases of pinned protocols through the pin:


Now invalidate pins on connector close:


Now wire the pin through `ClientRequest`. Update the type-checking import and add the request attribute:


Now wire it into `ClientSession`. First the import:


Set the active pin on each request built in the redirect loop:


Detect the 101 upgrade to exhaust the pin:


Now add the public `pin_connection()` method to `ClientSession`:


Export the new exception from `client.py` and add both symbols to the top-level package:


Let me verify the current on-disk state of the connector file to see what has been applied:


The previews are pending (file unchanged on disk), and the pending set is consistent (`contextvars.Token` annotation + `import contextvars`). Now let me add documentation. Let me find the relevant docs sections:


---

**User**

Continue

**Assistant**

Let me retrieve the most relevant prior experience before continuing the implementation.
The recalled experience IDs aren't available, and none of the listed topics (proxy rotation, auth sessions, WS keepalive, TCP server lifecycle) match this connector-level pinning design. I'll continue grounded in the actual source. Let me locate the docs insertion points for the session method and exception:


Now let me add a narrative section in the advanced client docs. Let me find the connectors/pooling section:


Let me verify how proxy auth is handled on fresh connections versus pooled reuse, to ensure pinned reuse matches:


Let me read the proxy connection setup to precisely determine proxy-auth handling for reuse:


Confirmed the changes are still proposals (the on-disk file is original), so my edits are self-contained. To keep pinned reuse proxy behavior identical to pooled reuse without altering the un-pinned path, I'll apply proxy authorization in the terminal send handler, gated on an active pin. Let me add that:


Now let me create a focused, self-contained test module covering both connector-level mechanics and end-to-end session behavior:


Interesting — that matched, meaning the file may already exist. Let me check:


The file doesn't exist; per the workflow, a new test file is provided as a code block. Here it is:

```python
# tests/test_connection_pin.py
"""Tests for ClientSession connection pinning scopes (ClientSession.pin_connection)."""

import asyncio
from collections import deque
from collections.abc import Callable
from unittest import mock

import pytest
from pytest_aiohttp import AiohttpClient
from yarl import URL

import aiohttp
from aiohttp import web
from aiohttp.client_exceptions import ClientPinnedConnectionError
from aiohttp.client_proto import ResponseHandler
from aiohttp.client_reqrep import ClientRequest, ClientTimeout, ConnectionKey
from aiohttp.connector import BaseConnector, current_connection_pin


def make_proto(*, should_close: bool = False, connected: bool = True) -> mock.Mock:
    loop = asyncio.get_running_loop()
    closed = loop.create_future()
    closed.set_result(None)
    proto: mock.Mock = mock.create_autospec(
        ResponseHandler,
        instance=True,
        should_close=should_close,
        closed=closed,
    )
    proto.is_connected.return_value = connected
    proto.should_close = should_close
    return proto


class FakeTrace:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def send_connection_queued_start(self) -> None:
        self.events.append("queued_start")

    async def send_connection_queued_end(self) -> None:
        self.events.append("queued_end")

    async def send_connection_reuseconn(self) -> None:
        self.events.append("reuseconn")


def seed(connector: BaseConnector, key: ConnectionKey, proto: mock.Mock) -> None:
    connector._conns[key] = deque([(proto, asyncio.get_running_loop().time())])


# --------------------------------------------------------------------------- #
# Connector level
# --------------------------------------------------------------------------- #


async def test_pin_binds_to_first_connection_and_reuses_it(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    key = req.connection_key
    proto = make_proto()
    seed(connector, key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin

    conn = await connector.connect(req, [], ClientTimeout())
    assert conn.protocol is proto
    assert pin.bound
    assert pin.key == key
    assert key not in connector._conns
    assert proto in connector._acquired

    conn.release()  # response finished, connection stays reserved by the pin
    assert key not in connector._conns  # never returned to the shared pool

    req2 = make_client_request("GET", URL("http://localhost:80"))
    req2._connection_pin = pin
    conn2 = await connector.connect(req2, [], ClientTimeout())
    assert conn2.protocol is proto
    conn2.release()

    await connector.close()


async def test_pin_waits_for_busy_connection_instead_of_opening_a_second(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    first = await connector.connect(req, [], ClientTimeout())

    req2 = make_client_request("GET", URL("http://localhost:80"))
    req2._connection_pin = pin
    second_task = asyncio.create_task(
        connector.connect(req2, [], ClientTimeout())
    )
    await asyncio.sleep(0)
    assert not second_task.done()

    first.release()  # releasing the pinned connection unblocks the waiter
    second = await second_task
    assert second.protocol is proto
    assert len(connector._acquired) == 1
    second.release()
    await connector.close()


async def test_pin_wait_honours_request_connect_timeout(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    held = await connector.connect(req, [], ClientTimeout())

    req2 = make_client_request("GET", URL("http://localhost:80"))
    req2._connection_pin = pin
    with pytest.raises(asyncio.TimeoutError):
        await connector.connect(
            req2, [], ClientTimeout(connect=0.01, total=5)
        )
    # No extra slot was consumed while waiting.
    assert len(connector._acquired) == 1
    held.release()
    await connector.close()


async def test_pin_fails_without_redial_when_connection_drops(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    conn = await connector.connect(req, [], ClientTimeout())
    conn.release()

    proto.is_connected.return_value = False

    req2 = make_client_request("GET", URL("http://localhost:80"))
    req2._connection_pin = pin
    with pytest.raises(ClientPinnedConnectionError):
        await connector.connect(req2, [], ClientTimeout())
    assert pin.invalid
    assert proto not in connector._acquired
    proto.close.assert_called()

    req3 = make_client_request("GET", URL("http://localhost:80"))
    req3._connection_pin = pin
    with pytest.raises(ClientPinnedConnectionError):
        await connector.connect(req3, [], ClientTimeout())
    await connector.close()


async def test_pin_different_key_uses_pool_and_keeps_pin_target(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req_a = make_client_request("GET", URL("http://localhost:80"))
    proto_a = make_proto()
    seed(connector, req_a.connection_key, proto_a)

    proto_b = make_proto()
    req_b = make_client_request("GET", URL("http://otherhost:80"))
    seed(connector, req_b.connection_key, proto_b)

    pin = connector._create_pin()
    req_a._connection_pin = pin
    conn_a = await connector.connect(req_a, [], ClientTimeout())
    assert conn_a.protocol is proto_a

    req_b._connection_pin = pin
    conn_b = await connector.connect(req_b, [], ClientTimeout())
    assert conn_b.protocol is proto_b  # not the pinned connection
    conn_b.release()
    # The other-key connection pools normally.
    assert proto_b in [p for q in connector._conns.values() for p, _ in q]
    # The pin still targets the original key/connection.
    assert pin.key == req_a.connection_key
    conn_a.release()
    await connector.close()


async def test_pin_closing_scope_returns_idle_connection_to_pool(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    conn = await connector.connect(req, [], ClientTimeout())
    conn.release()
    pin.close()

    assert proto in [p for q in connector._conns.values() for p, _ in q]
    # A plain, un-pinned request now reuses the returned connection.
    plain = make_client_request("GET", URL("http://localhost:80"))
    pooled = await connector.connect(plain, [], ClientTimeout())
    assert pooled.protocol is proto
    pooled.release()
    await connector.close()


async def test_pin_websocket_upgrade_exhausts_scope(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    conn = await connector.connect(req, [], ClientTimeout())

    waiter_req = make_client_request("GET", URL("http://localhost:80"))
    waiter_req._connection_pin = pin
    waiter = asyncio.create_task(
        connector.connect(waiter_req, [], ClientTimeout())
    )
    await asyncio.sleep(0)

    pin.notify_websocket_upgrade(proto)
    with pytest.raises(ClientPinnedConnectionError):
        await waiter

    next_req = make_client_request("GET", URL("http://localhost:80"))
    next_req._connection_pin = pin
    with pytest.raises(ClientPinnedConnectionError):
        await connector.connect(next_req, [], ClientTimeout())

    # WebSocket shutdown closes the (still acquired) pinned connection.
    conn.close()
    assert proto not in connector._acquired
    await connector.close()


async def test_pin_emits_queue_and_reuse_traces(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    first = await connector.connect(req, [], ClientTimeout())

    events: list[str] = []
    req2 = make_client_request("GET", URL("http://localhost:80"))
    req2._connection_pin = pin
    task = asyncio.create_task(
        connector.connect(req2, [FakeTrace(events)], ClientTimeout())
    )
    await asyncio.sleep(0)
    first.release()
    await task
    assert "queued_start" in events and "queued_end" in events
    assert "reuseconn" in events
    await connector.close()


async def test_pin_connector_close_cancels_waiters(
    make_client_request: Callable[..., ClientRequest],
) -> None:
    connector = aiohttp.BaseConnector()
    req = make_client_request("GET", URL("http://localhost:80"))
    proto = make_proto()
    seed(connector, req.connection_key, proto)

    pin = connector._create_pin()
    req._connection_pin = pin
    held = await connector.connect(req, [], ClientTimeout())

    req2 = make_client_request("GET", URL("http://localhost:80"))
    req2._connection_pin = pin
    waiter = asyncio.create_task(
        connector.connect(req2, [], ClientTimeout())
    )
    await asyncio.sleep(0)
    await connector.close()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    held.release()


async def test_nested_pins_restore_outer_scope() -> None:
    connector = aiohttp.BaseConnector()
    outer = connector._create_pin()
    inner = connector._create_pin()
    assert current_connection_pin.get() is None
    async with outer:
        assert current_connection_pin.get() is outer
        async with inner:
            assert current_connection_pin.get() is inner
        assert current_connection_pin.get() is outer
    assert current_connection_pin.get() is None
    await connector.close()


# --------------------------------------------------------------------------- #
# End to end against a real server
# --------------------------------------------------------------------------- #


def _pin_app() -> web.Application:
    app = web.Application()
    app["transports"] = set()

    async def handler(request: web.Request) -> web.Response:
        app["transports"].add(request.transport)
        return web.Response(text=str(request.transport.get_extra_info("peername")))

    async def slow(request: web.Request) -> web.Response:
        app["transports"].add(request.transport)
        await asyncio.sleep(0.15)
        return web.Response(text=str(request.transport.get_extra_info("peername")))

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for msg in ws:
            await ws.send_str(msg.data + "/answer")
        return ws

    app.router.add_get("/", handler)
    app.router.add_get("/slow", slow)
    app.router.add_get("/ws", ws_handler)
    return app


async def test_session_pin_reuses_one_transport(aiohttp_client: AiohttpClient) -> None:
    client = await aiohttp_client(_pin_app())
    async with client.pin_connection():
        async with client.get("/") as resp:
            first = await resp.text()
        async with client.get("/") as resp:
            second = await resp.text()
        async with client.get("/") as resp:
            third = await resp.text()
    assert first == second == third


async def test_session_pin_concurrent_requests_share_and_serialize(
    aiohttp_client: AiohttpClient,
) -> None:
    client = await aiohttp_client(_pin_app())

    async def one() -> str:
        async with client.pin_connection():
            async with client.get("/slow") as resp:
                return await resp.text()

    results = await asyncio.gather(one(), one())
    assert results[0] != results[1]  # two independent scopes, two connections
    transports = client.server.app["transports"]
    assert len(transports) == 2

    # Inside a single pin, concurrent same-key calls still hit one transport.
    async with client.pin_connection():
        async def call() -> str:
            async with client.get("/slow") as resp:
                return await resp.text()

        pinned = await asyncio.gather(call(), call())
    assert pinned[0] == pinned[1]


async def test_session_pin_fails_after_server_disconnect(
    aiohttp_client: AiohttpClient,
) -> None:
    client = await aiohttp_client(_pin_app())
    async with client.pin_connection() as pin:
        async with client.get("/") as resp:
            await resp.text()
        # Force the pinned transport down between requests.
        assert pin._protocol is not None
        transport = pin._protocol.transport
        assert transport is not None
        transport.close()
        with pytest.raises(ClientPinnedConnectionError):
            await client.get("/")


async def test_session_pin_websocket_exhausts_scope(
    aiohttp_client: AiohttpClient,
) -> None:
    client = await aiohttp_client(_pin_app())
    async with client.pin_connection():
        ws = await client.ws_connect("/ws")
        await ws.send_str("hi")
        msg = await ws.receive()
        assert msg.data == "hi/answer"
        await ws.close()
        with pytest.raises(ClientPinnedConnectionError):
            await client.get("/")


async def test_requests_outside_pin_are_unaffected(aiohttp_client: AiohttpClient) -> None:
    client = await aiohttp_client(_pin_app())
    async with client.pin_connection():
        async with client.get("/") as resp:
            pinned = await resp.text()
    # After the scope closes, ordinary pooling keeps working.
    async with client.get("/") as resp:
        pooled = await resp.text()
    assert isinstance(pinned, str) and isinstance(pooled, str)
```

Now the changelog fragment (new file):

```rst
CHANGES/14100.feature.rst
Added ``ClientSession.pin_connection()`` -- an asynchronous context manager
that pins requests with the same connection key (host, port, proxy and TLS)
to the single connection used by the first request in the scope. Same-key
requests wait for that connection instead of opening a second one, and fail
with the new ``aiohttp.ClientPinnedConnectionError`` rather than reconnecting
if the pinned connection is closed or taken over by a WebSocket upgrade.
Requests for other keys and all requests outside a scope keep using the
regular connection pool unchanged.
-- by :user:`please-fill-github-handle`
```

Let me do a final self-review of a couple of subtle points in the implementation by re-reading the proposed `acquire` waiting loop and its interaction with cancellation of the *in-flight* (first) request. Let me also double-check that `ConnectionPin.__aexit__` is declared `async` but the user writes `async with session.pin_connection()`, which requires `__aenter__`/`__aexit__` to be coroutine functions — yes they are.

Let me verify there are no name collisions: `current_connection_pin` defined in connector.py after imports; referenced inside `ConnectionPin.__aenter__` at call time — module global, fine. `ConnectionKey` forward-ref used in annotations at class-body evaluation time — annotations on method signatures are evaluated at function definition time (unless `from __future__ import annotations`). connector.py does NOT have `from __future__ import annotations`. So annotations like `key: "ConnectionKey"` are strings (quoted) — I quoted them. But I also have unquoted references? Let me audit the class I proposed:

- `__init__(self, connector: "BaseConnector", loop: asyncio.AbstractEventLoop)` quoted BaseConnector ✓
- `_key: ConnectionKey | None = None` — UNQUOTED class-variable annotation inside `__slots__` class. Annotations on assignments inside a method body (`self._key: ConnectionKey | None = None`) are evaluated? Local variable annotations in function bodies are NOT evaluated (they're stored in __annotations__ only for module/class; for local vars the annotation is not evaluated). Per PEP 526, local variable annotations are not evaluated. So `self._key: ConnectionKey | None = None` is safe (annotation ignored at runtime). ✓
- `_waiters: deque[asyncio.Future[None]] = deque()` — local annotation, not evaluated ✓
- `_token: contextvars.Token[...]` local annotation ✓ (needs `import contextvars` — added).
- Method signatures: `async def acquire(self, req: ClientRequest, key: "ConnectionKey", traces: list["Trace"], timeout: "ClientTimeout") -> Connection:` — RETURN annotation `Connection` is UNQUOTED and evaluated at def time! `Connection` class is defined ABOVE `ConnectionPin`? Order in file: `Connection` (line 178) comes before `BaseConnector` (305) where I inserted `ConnectionPin` immediately before `BaseConnector`. So `Connection` is already defined at class-definition time ✓. `ClientRequest` is imported at top ✓. So return annotation Connection resolves ✓.
- `_error(self) -> ClientPinnedConnectionError:` — imported at top ✓.
- `_on_release(self, key: "ConnectionKey", protocol: ResponseHandler, ...)` ResponseHandler imported ✓, key quoted ✓.
- `notify_websocket_upgrade(self, protocol: ResponseHandler)` ✓.
- `_teardown(self, protocol: ResponseHandler, *, ...)` ✓.
- `async def _wait_for_release(self, traces: list["Trace"])` ✓.
- `bound(self) -> bool`, `key(self) -> "ConnectionKey | None"` quoted ✓.
- `__aexit__` uses TracebackType (imported at top) ✓.
- `_waiters` type in __slots__ fine.

`async def acquire(...) -> Connection:` — Connection defined earlier, good. `list["Trace"]` fine.

In `connect()` dispatcher: `pin = getattr(req, "_connection_pin", None)` no annotation issues. `pin._connector is not self` fine.

`_create_pin` return annotation `-> ConnectionPin` — inside BaseConnector body, ConnectionPin already defined above ✓.

The module-level `current_connection_pin: ContextVar["ConnectionPin | None"] = ContextVar(...)` — ConnectionPin defined LATER (I placed the ContextVar near top after __all__, but ConnectionPin is defined just before BaseConnector ~line 305). The annotation `ContextVar["ConnectionPin | None"]` is a string (subscript with string) → not evaluated; fine. The RHS `ContextVar("aiohttp_current_connection_pin", default=None)` doesn't reference ConnectionPin ✓.

ClientSession.pin_connection return annotation `-> ConnectionPin`: ConnectionPin imported in client.py ✓.

client_ws / others unaffected.

Now, one more: in `connect()` I call `pin.acquire(req, key, traces, timeout)` but the existing call signature is positional `connect(req, traces, timeout)` and tests call `conn.connect(req, traces, ClientTimeout())`. My dispatcher signature unchanged ✓.

Also `getattr(req, "_connection_pin", None)` — ClientRequestBase class attr default None exists after client_reqrep edit ✓.

Potential mypy: `getattr` returns Any; dispatch fine.

Now check `_connect_unpinned` — I renamed original `connect` body; the class insertion diff for the dispatcher changed the def line and added wrappers, keeping the rest (starting `key = req.connection_key`). The body references `self`, works as method. ✓.

Also ensure I did not leave a duplicate `"""Get from pool or create new connection."""` mismatch: the dispatcher got a new docstring, `_connect_unpinned` got the original one. ✓.

Finally, the `client.py` `__all__` — I added "ClientPinnedConnectionError" and "ConnectionPin". The top-level __init__ imports ConnectionPin from `.client` — but ConnectionPin is DEFINED in connector.py and merely imported into client.py. Re-exporting it through client is fine (it's in client namespace and client __all__). __init__ also imports ClientPinnedConnectionError from `.client` — re-exported there ✓.

Let me verify the __init__ import block: ConnectionPin added inside the `from .client import (...)` alphabetically near ClientWSTimeout/ConnectionTimeoutError — and the exception added in same block. Both are names available in client.py ✓.

I think the implementation is complete and coherent. Let me write the final summary, noting tests must be run and the changelog number/handle adjusted. I cannot execute pytest here (read-only), so I'll provide the command per AGENTS.md.

Let me also reconsider requirement: "两层范围叠在一起时，内层只影响内层，关掉内层后外层仍然钉着原来的连接" — contextvar reset handles scope selection; but inner and outer pins each bind their own connections independently. If inner requests go to the SAME key as outer while outer's connection is idle, the inner pin calls _connect_unpinned — will it accidentally pick the OUTER pinned proto? Outer proto is reserved (not in _conns, in _acquired), so pool won't hand it; inner creates/gets a different connection. Limit permitting. With limit=1 it could block waiting for a slot — edge case, acceptable (same as using two connections with limit 1 generally). Inner uses its own connection; outer unaffected. ✓

"同一个会话上两个并发范围不能串连接" — each pin reserves distinct prots; pool won't serve a reserved proto (kept out of _conns). ✓

"连接获取、复用和排队的追踪回调仍然要触发" — reuseconn + queued start/end wired; create_start/end via unpinned bind. ✓

I'm confident. Final answer summary.All changes are proposed as reviewable diffs (plus the two new files as code blocks above). Since I'm in read-only Chat Mode, please **review and accept each diff**, create the two new files, then run the tests. Here's a summary of the design and implementation.

## What was added

A new public async context manager on the session:

```python
async with session.pin_connection():
    async with session.get("http://host/a") as r: ...
    async with session.get("http://host/b") as r: ...  # same TCP connection
```

**New API**
- `aiohttp.ClientSession.pin_connection()` — opens a pinning scope (`client.py:1409` area).
- `aiohttp.ConnectionPin` — the scope object / async context manager (`aiohttp/connector.py`).
- `aiohttp.ClientPinnedConnectionError` — raised when the pinned connection is gone (`aiohttp/client_exceptions.py`).

## How it works (pinning lives on the acquire/release path)

- A `ContextVar` (`current_connection_pin`) carries the innermost open scope. `ClientSession._request` stamps every built request — including each redirect hop — with `req._connection_pin`.
- `BaseConnector.connect()` now dispatches: a stamped, same-key request goes to `pin.acquire(...)`; everything else calls the **unchanged** original pool logic, renamed `_connect_unpinned()` (`connector.py:663`). Behavior with no scope is therefore byte-for-byte the same (limits, keepalive, fingerprints, proxy, SSL, cookies, timeouts, redirects, WebSocket).
- The pin starts **unbound**; the opening request obtains a connection normally (fresh or idle-pooled), then reserves that exact protocol and key. Pinned protocols stay in `_acquired` (so limits/quota and fingerprint checks on create are preserved) but are kept **out of `_conns`** via `_pin_of_protocol`, so no other request or scope can ever borrow them.
- Later same-key requests reuse that protocol and fire `connection_reuseconn`; if it's still busy they wait on the pin's FIFO (firing `connection_queued_start/end`) bounded by that request's own `connect` timeout, never consuming an extra slot and never dialing a second connection. Cancellation/timeout just drop the waiter.
- `BaseConnector._release()` routes a pinned protocol back to its pin (`_on_release`) instead of pooling it. On close/failure the pin invalidates, releases quota and wakes waiters with `ClientPinnedConnectionError`; it never reconnects.
- Redirects staying on the same key remain on the pinned connection; a different key uses the pool and leaves the pin target unchanged. A `101` upgrade calls `notify_websocket_upgrade()`, exhausting the scope (later same-key requests fail); WebSocket shutdown then closes the protocol.
- Nested scopes reset the context var on exit (inner can't affect outer), concurrent scopes hold distinct protocols, and connector close cancels pin waiters and closes pinned protocols under the existing rules.

## Files

- `aiohttp/client_exceptions.py` — new exception
- `aiohttp/connector.py` — `ConnectionPin`, context var, pin registry, dispatcher + release/close routing
- `aiohttp/client_reqrep.py` — `ClientRequestBase._connection_pin`
- `aiohttp/client.py` — `pin_connection()`, per-request stamping, 101 handling, pinned proxy-auth parity, exports
- `aiohttp/__init__.py` — public exports
- `docs/client_reference.rst`, `docs/client_advanced.rst` — reference + narrative
- New: `tests/test_connection_pin.py` (connector mechanics + real-server tests), `CHANGES/14100.feature.rst` (please replace the number/`:user:` handle)

## Verify

```powershell
$env:PYTHONPATH='.'
pytest tests/test_connection_pin.py -q
pytest tests/test_connector.py tests/test_client_session.py tests/test_client_functional.py tests/test_client_ws_functional.py -q
```

Per `AGENTS.md`, please also run the parser/websocket-related tests with the Cython extensions installed, and `pre-commit run --files aiohttp/connector.py aiohttp/client.py aiohttp/client_reqrep.py aiohttp/client_exceptions.py aiohttp/__init__.py`.

One note worth confirming during human review: while a scope is open an idle pinned connection is intentionally **not** swept by the keep-alive timer (it isn't in `_conns`); a genuinely dropped socket is detected on next reuse via `is_connected()` and fails with `ClientPinnedConnectionError`, matching the "never silently redial" requirement.

---

