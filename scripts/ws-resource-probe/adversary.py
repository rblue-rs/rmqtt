"""Independent local raw-WS adversary; not a broker, production framing, or RSS proof."""
from pathlib import Path
import argparse,base64,hashlib,json,os,socket,ssl,struct,subprocess,sys,tempfile,threading,time
ROOT=Path(__file__).resolve().parents[2]
p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--cases',default='header,frame,fragment,tiny,ping,slow,backpressure,close,drop,drain,quota-count,quota-bytes');a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
def rl(n):
 b=bytearray()
 while True:
  x=n%128;n//=128;b.append(x|(128 if n else 0))
  if not n:return bytes(b)
def exact(s,n):
 b=bytearray()
 while len(b)<n:
  v=s.recv(n-len(b))
  if not v:raise EOFError()
  b.extend(v)
 return bytes(b)
def frame(s,op,b):
 n=len(b);s.sendall(bytes([op,n]) if n<126 else bytes([op,126])+struct.pack('!H',n) if n<=65535 else bytes([op,127])+struct.pack('!Q',n));s.sendall(b)
def readframe(s):
 h=exact(s,2);n=h[1]&127
 if n==126:n=struct.unpack('!H',exact(s,2))[0]
 if n==127:n=struct.unpack('!Q',exact(s,8))[0]
 assert n<=2*1024*1024 and h[1]&128,'invalid/oversized fixture client frame'
 mask=exact(s,4);b=exact(s,n);return h[0],bytes(v^mask[i%4] for i,v in enumerate(b))
def mqtt(s):
 while True:
  op,b=readframe(s)
  if op&15 in (9,10):continue
  assert op==0x82
  i=1
  while b[i]&128:i+=1
  return b[0],b[i+1:]
def publish(protocol,payload):
 body=b'\0\1q'+(b'\0' if protocol==5 else b'')+payload
 return b'\x30'+rl(len(body))+body
rows=[]
with tempfile.TemporaryDirectory(prefix='mqtt-ws-adversary-') as tmp:
 certs=Path(tmp);subprocess.run([sys.executable,str(ROOT/'scripts/gen_test_certs.py'),tmp],check=True,stdout=subprocess.DEVNULL)
 for protocol in [4,5]:
  for tls in [False,True]:
   for case in a.cases.split(','):
    listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(8);port=listener.getsockname()[1];ready=threading.Event();replacement=threading.Event();force=threading.Event();errors=[];observed={'subscribes':0,'unsubscribes':0,'disconnect':False,'forbidden_seen':False}
    def peer():
     try:
      sock,_=listener.accept();sock.settimeout(8);sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
      if tls:
       context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(certs/'server-127.pem',certs/'server-127.key');sock=context.wrap_socket(sock,server_side=True)
      with sock as s:
       h=b''
       while b'\r\n\r\n' not in h:
        h+=exact(s,1);assert len(h)<=8192
       lines=h.decode().split('\r\n');assert lines[0].startswith('GET /mqtt ');headers={k.lower():v.strip() for k,v in (x.split(':',1) for x in lines[1:] if ':' in x)};assert headers['sec-websocket-protocol']=='mqtt';accept=base64.b64encode(hashlib.sha1((headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
       s.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Protocol: mqtt\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n');op,b=mqtt(s);assert op==0x10 and b[6]==protocol;frame(s,0x82,b'\x20\x03\x00\x00\x00' if protocol==5 else b'\x20\x02\x00\x00');assert ready.wait(5)
       if case=='header':s.sendall(b'\x82\x7f'+struct.pack('!Q',17*1024*1024))
       elif case in ('frame','fragment'):
        n=(3 if case=='frame' else 4)*1024*1024;data=publish(protocol,b'\x00'*(n-16));
        if case=='frame':frame(s,0x82,data)
        else:
         parts=[data[i:i+128*1024] for i in range(0,len(data),128*1024)]
         for i,v in enumerate(parts):frame(s,2 if i==0 else 0x80 if i==len(parts)-1 else 0,v)
       elif case=='tiny':
        for b in publish(protocol,b'\x00\xff\x01'):frame(s,0x82,bytes([b]))
       elif case=='ping':
        for _ in range(64):frame(s,0x89,b'x'*125)
        frame(s,0x82,publish(protocol,b'\x00\xff\x01'))
       elif case=='slow':
        data=publish(protocol,b'\x00'*65536)
        for _ in range(129):frame(s,0x82,data)
       elif case in ('close','drop','drain'):frame(s,2,b'\x00'*(512*1024))
       elif case=='backpressure':
        s.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,4096);assert force.wait(8);return
       while True:
        try:op,b=mqtt(s)
        except ConnectionResetError:
         assert case in ('close','drop'),case;observed['force_reset']=True;return
        except (EOFError,ssl.SSLEOFError):
         assert case in ('header','frame','fragment','slow','close','drop'),case;return
        if op==0xc0:frame(s,0x82,b'\xd0\x00');continue
        if op==0xe0:observed['disconnect']=True;return
        if op in (0x82,0xa2):
         offset=3 if protocol==5 else 2;n=int.from_bytes(b[offset:offset+2],'big');topic=b[offset+2:offset+2+n];assert len(topic)==n
         if topic.startswith(b'forbidden/') or (case=='quota-bytes' and topic==b'z'):
          observed['forbidden_seen']=True;raise AssertionError('quota rejection reached wire')
         if op==0x82:
          observed['subscribes']+=1;body=b[:2]+(b'\0' if protocol==5 else b'')+bytes([b[-1]&3]);frame(s,0x82,b'\x90'+rl(len(body))+body)
         else:
          observed['unsubscribes']+=1;assert replacement.wait(5),'replacement admission fence missing';body=b[:2]+(b'\0\0' if protocol==5 else b'');frame(s,0x82,b'\xb0'+rl(len(body))+body)
        else:raise AssertionError(f'unexpected packet {op:#x}')
     except BaseException as e:errors.append(f'{type(e).__name__}: {e}')
     finally:listener.close()
    thread=threading.Thread(target=peer);thread.start();cmd=[str(a.binary.resolve()),'--client',str(port),str(protocol),'1' if tls else '0',str(certs/'ca.pem'),case];proc=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);output=[]
    def collect():
     for line in proc.stdout:
      output.append(line)
      if line.strip()=='CLIENT_READY':ready.set()
      if line.strip()=='REPLACEMENT_ADMITTED':replacement.set()
      if line.strip()=='FORCE_CLOSED':force.set()
    reader=threading.Thread(target=collect);reader.start();thread.join(20)
    if thread.is_alive():proc.kill();errors.append('peer timeout')
    elif case=='slow' and proc.poll() is None:proc.stdin.write('PEER_EOF\n');proc.stdin.flush()
    try:exit=proc.wait(timeout=15)
    except subprocess.TimeoutExpired:proc.kill();exit=proc.wait();errors.append('client timeout')
    force.set();thread.join(10);reader.join(5);assert not thread.is_alive() and not reader.is_alive()
    name=f'p{protocol}-'+('wss' if tls else 'ws')+'-'+case;raw=''.join(output);(a.output/(name+'.log')).write_text(raw);row={'protocol':protocol,'transport':'wss' if tls else 'ws','case':case,'exit':exit,'peer_errors':errors,'observed':observed};rows.append(row);(a.output/'results.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)
print('adversarial product observations',sum(r['exit']==0 and not r['peer_errors'] for r in rows),'/',len(rows))
sys.exit(0 if all(r['exit']==0 and not r['peer_errors'] for r in rows) else 1)
