#!/usr/bin/env python3
"""Run a command against a temporary independent aMQTT MQTT 3.1.1 broker.
No public port, account, external broker, or persistent service. Requires amqtt.
"""
import asyncio
import os
import socket
import sys
from amqtt.broker import Broker

async def main():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    broker = Broker({
        'listeners': {'default': {'type': 'tcp', 'bind': f'127.0.0.1:{port}'}},
        'plugins': {'amqtt.plugins.authentication.AnonymousAuthPlugin': {'allow_anonymous': True}},
    })
    await broker.start()
    print(f'Temporary aMQTT 0.12.2 broker: 127.0.0.1:{port}', flush=True)
    env = dict(os.environ, MQTT_TEST_PORT=str(port))
    try:
        process = await asyncio.create_subprocess_exec(*sys.argv[1:], env=env)
        return await asyncio.wait_for(process.wait(), 120)
    finally:
        if 'process' in locals() and process.returncode is None:
            process.kill()
            await process.wait()
        await broker.shutdown()

if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
