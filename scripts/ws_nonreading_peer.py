#!/usr/bin/env python3
"""Bounded independent WS peer. Reads CONNECT only, then never reads again."""
import sys,socket,ssl,hashlib,base64
s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,4096);s.bind(('127.0.0.1',0));s.listen(1)
print(s.getsockname()[1],flush=True)
c,_=s.accept();c.settimeout(8)
if len(sys.argv)>1:
 ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(sys.argv[1],sys.argv[2]);c=ctx.wrap_socket(c,server_side=True)
def exact(n):
 assert n<=4096
 b=bytearray()
 while len(b)<n:
  z=c.recv(n-len(b));assert z;b.extend(z)
 return bytes(b)
h=bytearray()
while not h.endswith(b'\r\n\r\n'):
 assert len(h)<4096;h.extend(exact(1))
headers=dict(x.split(b': ',1) for x in bytes(h).split(b'\r\n')[1:] if b': ' in x)
a=base64.b64encode(hashlib.sha1(headers[b'Sec-WebSocket-Key']+b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
c.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Protocol: mqtt\r\nSec-WebSocket-Accept: '+a+b'\r\n\r\n')
f=exact(2);assert f[0]==0x82 and f[1]&128
n=f[1]&127
if n==126:n=int.from_bytes(exact(2),'big')
assert n<1024
mask=exact(4);body=bytes(x^mask[i%4] for i,x in enumerate(exact(n)));assert body[0]==0x10
# Find CONNECT protocol level using its Remaining Length prefix.
i=1
while body[i]&128:i+=1
i+=1;v=body[i+6]
ack=bytes([32,3,0,0,0]) if v==5 else bytes([32,2,0,0])
c.sendall(bytes([0x82,len(ack)])+ack)
print('nonreading',flush=True)
# No WebSocket library/background reader; bounded single control-line ownership.
assert sys.stdin.readline().strip()=='exit'
c.close();s.close()
