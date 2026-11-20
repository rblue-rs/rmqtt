#!/usr/bin/env python3
"""Run a command against a temporary independent aMQTT MQTT 3.1.1 broker.
No public port, account, external broker, or persistent service. Requires amqtt.

Plain TCP is the default. When MQTT_TEST_TLS_CERT and MQTT_TEST_TLS_KEY are
set, a TLS listener is also started (and a plaintext listener is kept on a
separate port). When MQTT_TEST_TLS_CA is set with the TLS cert/key pair, that CA
is used to verify client certificates. MQTT_TEST_REQUIRE_CLIENT_CERT=1 requires a
valid client certificate (mTLS). When MQTT_TEST_AUTH_USERNAME and
MQTT_TEST_AUTH_PASSWORD are set together, anonymous access is disabled and only
that pair is accepted. Credentials, certificates, and private keys are never
written to stdout, the ready file, or log lines.
--serve READY_FILE holds the broker until stdin closes or SIGTERM, writing
port= and optional plain_port= to READY_FILE.
"""
import asyncio
import hmac
import logging
import os
import signal
import socket
import sys
from dataclasses import dataclass, field

from amqtt.broker import Broker
from amqtt.contexts import BaseContext
from amqtt.plugins.base import BaseAuthPlugin
from amqtt.session import Session

logging.basicConfig(level=logging.WARNING)

# Importable name for amqtt's dotted-path plugin loader when this file is
# executed as a script (__name__ == "__main__").
sys.modules.setdefault("mqtt_test_broker", sys.modules[__name__])


class FixedPasswordAuthPlugin(BaseAuthPlugin):
    """Accept a single in-process username/password pair for tests."""

    @dataclass
    class Config:
        username: str = ""
        password: str = field(default="", repr=False)

    def __init__(self, context: BaseContext) -> None:
        super().__init__(context)
        self._username = self._get_config_option("username", "")
        self._password = self._get_config_option("password", "")

    async def authenticate(self, *, session: Session) -> bool:
        if not session or session.username is None:
            return False
        user_ok = hmac.compare_digest(session.username, self._username)
        provided = session.password or ""
        pass_ok = hmac.compare_digest(provided, self._password)
        return bool(user_ok and pass_ok)


def bind_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def broker_settings(plain_port, tls_port=None, cert=None, key=None, username=None, password=None, cafile=None, require_client_cert=False):
    listeners = {
        'default': {'type': 'tcp', 'bind': f'127.0.0.1:{plain_port}'},
    }
    if tls_port is not None:
        tls = {
            'type': 'tcp',
            'bind': f'127.0.0.1:{tls_port}',
            'ssl': True,
            'certfile': cert,
            'keyfile': key,
        }
        if cafile:
            tls['cafile'] = cafile
        if require_client_cert:
            tls['client_cert'] = 'required'
        listeners['tls'] = tls
    if username is not None:
        plugins = {
            'mqtt_test_broker.FixedPasswordAuthPlugin': {
                'username': username,
                'password': password,
            }
        }
    else:
        plugins = {
            'amqtt.plugins.authentication.AnonymousAuthPlugin': {
                'allow_anonymous': True
            }
        }
    return {
        'listeners': listeners,
        'plugins': plugins,
    }


def tls_files():
    cert = os.environ.get('MQTT_TEST_TLS_CERT')
    key = os.environ.get('MQTT_TEST_TLS_KEY')
    if bool(cert) != bool(key):
        raise SystemExit('MQTT_TEST_TLS_CERT and MQTT_TEST_TLS_KEY must be set together')
    if cert and key:
        return cert, key
    return None, None


def tls_client_auth():
    ca = os.environ.get('MQTT_TEST_TLS_CA')
    require = os.environ.get('MQTT_TEST_REQUIRE_CLIENT_CERT', '')
    require_client_cert = require.lower() in ('1', 'true', 'yes')
    if require_client_cert and not ca:
        raise SystemExit('MQTT_TEST_REQUIRE_CLIENT_CERT requires MQTT_TEST_TLS_CA')
    return ca, require_client_cert


def auth_pair():
    username = os.environ.get('MQTT_TEST_AUTH_USERNAME')
    password = os.environ.get('MQTT_TEST_AUTH_PASSWORD')
    if bool(username) != bool(password):
        raise SystemExit(
            'MQTT_TEST_AUTH_USERNAME and MQTT_TEST_AUTH_PASSWORD must be set together'
        )
    if username and password:
        return username, password
    return None, None


def write_ready(path, plain_port, tls_port):
    text = [f'plain_port={plain_port}\n']
    if tls_port is not None:
        text.insert(0, f'port={tls_port}\n')
        text.append(f'tls_port={tls_port}\n')
    else:
        text.insert(0, f'port={plain_port}\n')
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='ascii') as fh:
        fh.writelines(text)
    os.replace(tmp, path)


async def serve(ready_path, broker):
    loop = asyncio.get_running_loop()
    closed = asyncio.Event()

    def stop(*_):
        closed.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    async def wait_stdin():
        await asyncio.to_thread(sys.stdin.read)
        closed.set()

    stdin_task = asyncio.create_task(wait_stdin())
    try:
        await closed.wait()
    finally:
        stdin_task.cancel()
        await broker.shutdown()


async def main():
    serve_ready = None
    argv = sys.argv[1:]
    if argv[:1] == ['--serve']:
        if len(argv) != 2:
            raise SystemExit('usage: with_broker.py --serve READY_FILE')
        serve_ready = argv[1]
        argv = []
    elif not argv:
        raise SystemExit('usage: with_broker.py COMMAND [ARGS...] | --serve READY_FILE')

    cert, key = tls_files()
    username, password = auth_pair()
    cafile, require_client_cert = tls_client_auth()
    if (cafile or require_client_cert) and not cert:
        raise SystemExit('MQTT_TEST_TLS_CA requires MQTT_TEST_TLS_CERT and MQTT_TEST_TLS_KEY')
    plain_port = bind_port()
    tls_port = bind_port() if cert else None
    broker = Broker(broker_settings(plain_port, tls_port, cert, key, username, password, cafile, require_client_cert))
    await broker.start()
    env = dict(os.environ, MQTT_TEST_PORT=str(plain_port), MQTT_TEST_PLAIN_PORT=str(plain_port))
    if tls_port is not None:
        # Child tests that opt into TLS read MQTT_TEST_TLS_PORT; plaintext
        # tests keep using MQTT_TEST_PORT so existing fixtures stay valid.
        env['MQTT_TEST_TLS_PORT'] = str(tls_port)
        print(
            f'Temporary aMQTT 0.12.2 broker: 127.0.0.1:{plain_port} tls:127.0.0.1:{tls_port}',
            flush=True,
        )
    else:
        print(f'Temporary aMQTT 0.12.2 broker: 127.0.0.1:{plain_port}', flush=True)

    if serve_ready:
        write_ready(serve_ready, plain_port, tls_port)
        try:
            await serve(serve_ready, broker)
        finally:
            try:
                os.remove(serve_ready)
            except FileNotFoundError:
                pass
        return 0

    try:
        process = await asyncio.create_subprocess_exec(*argv, env=env)
        return await asyncio.wait_for(process.wait(), 120)
    finally:
        if 'process' in locals() and process.returncode is None:
            process.kill()
            await process.wait()
        await broker.shutdown()


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
