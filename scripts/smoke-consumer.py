#!/usr/bin/env python3
"""Test module-proxy + vendor consumption without Rust, Git submodules or .so files."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--packages', choices=['httpclient', 'websocket', 'both'], default='both')
parser.add_argument('--arch', choices=['arm64','amd64'], default=subprocess.check_output(['go','env','GOARCH'],text=True).strip())
parser.add_argument('--cc', help='cross C compiler, if needed by the maintainer running this test')
parser.add_argument('--runner', default='', help='optional emulator command for the consumer binary')
args = parser.parse_args()
(ROOT/'target').mkdir(exist_ok=True)
# Go's ./... package walk ignores underscore-prefixed directories, including
# module caches being removed while another test suite runs concurrently.
work = Path(tempfile.mkdtemp(prefix='_consumer-', dir=ROOT/'target'))
proxy = work/'proxy'
consumer = work/'app'
consumer.mkdir()
version = 'v0.0.0-prebuilt'
modules = []
for project in [ROOT]:
    module = (project/'go.mod').read_text().splitlines()[0].split()[1]
    modules.append(module)
    folder = proxy/module/'@v'
    folder.mkdir(parents=True)
    (folder/(version+'.mod')).write_bytes((project/'go.mod').read_bytes())
    (folder/(version+'.info')).write_text(json.dumps({'Version':version,'Time':'2026-09-19T00:00:00Z'}))
    (folder/'list').write_text(version+'\n')
    files = [project/'go.mod', *project.glob('*LICENSE*'), *project.glob('*NOTICES*')]
    for directory in ['httpclient', 'websocket', 'internal', 'native']:
        files += [p for p in (project/directory).rglob('*') if p.is_file() and p.suffix in ['.go','.h','.a','.json']]
    if (project/'go.sum').exists(): files.append(project/'go.sum')
    with zipfile.ZipFile(folder/(version+'.zip'),'w',zipfile.ZIP_DEFLATED) as archive:
        for file in files:
            archive.write(file, module+'@'+version+'/'+str(file.relative_to(project)))
(consumer/'go.mod').write_text('module consumer.example/app\n\ngo 1.27.0\n\nrequire (\n'+''.join('\t'+m+' '+version+'\n' for m in modules)+')\n')
imports=[]
checks=[]
for package in (['httpclient', 'websocket'] if args.packages == 'both' else [args.packages]):
    if package == 'httpclient':
        imports.append('h "'+modules[0]+'/httpclient"')
        checks.append('''hc, err := h.NewClient(h.Options{}); must(err); defer hc.Close()
    response, err := hc.Get(ctx, server.URL+"/http"); must(err)
    body, err := io.ReadAll(response.Body); must(err); must(response.Body.Close())
    if string(body)!="native-http" { panic("HTTP body mismatch") }
    fmt.Println("HTTP native archive OK")
    transport, err := h.NewTransport(h.Options{}); must(err)
    standardClient := &http.Client{Transport:transport, Timeout:10*time.Second}
    standardResponse, err := standardClient.Get(server.URL+"/http"); must(err)
    standardBody, err := io.ReadAll(standardResponse.Body); must(err); must(standardResponse.Body.Close())
    if string(standardBody)!="native-http" { panic("RoundTripper body mismatch") }
    fmt.Println("net/http RoundTripper OK")
    proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !r.URL.IsAbs() || r.URL.Host != "unresolvable.invalid" { panic("proxy target mismatch") }
        _, _ = io.WriteString(w, "native-proxy")
    })); defer proxy.Close()
    proxyTransport, err := h.NewTransport(h.Options{ProxyURL:proxy.URL}); must(err)
    proxyResponse, err := (&http.Client{Transport:proxyTransport, Timeout:10*time.Second}).Get("http://unresolvable.invalid/"); must(err)
    proxyBody, err := io.ReadAll(proxyResponse.Body); must(err); must(proxyResponse.Body.Close())
    if string(proxyBody)!="native-proxy" { panic("proxy body mismatch") }
    fmt.Println("explicit HTTP proxy OK")''')
    else:
        imports.append('ws "'+modules[0]+'/websocket"')
        checks.append('''wc, err := ws.NewClient(ws.Options{NoProxy:true}); must(err); defer wc.Close()
    conn, _, err := wc.Dial(ctx, "ws"+strings.TrimPrefix(server.URL,"http")+"/ws",nil); must(err); defer conn.Close()
    must(conn.WriteControl(ctx, ws.PingMessage, []byte("probe")))
    pongKind, pongData, err := conn.ReadMessage(ctx); must(err)
    if pongKind!=ws.PongMessage || string(pongData)!="probe" { panic("control frame mismatch") }
    must(conn.WriteMessage(ctx, ws.TextMessage, []byte("native-websocket")))
    kind, data, err := conn.ReadMessage(ctx); must(err)
    if kind!=ws.TextMessage || string(data)!="native-websocket" { panic("WebSocket echo mismatch") }
    _, rejected, err := wc.Dial(ctx, "ws"+strings.TrimPrefix(server.URL,"http")+"/http",nil)
    if _, ok := err.(*ws.HandshakeError); !ok || rejected==nil || rejected.StatusCode!=200 || string(rejected.Body)!="native-http" { panic("handshake metadata mismatch") }
    fmt.Println("WebSocket native archive, controls and handshake errors OK")''')
(consumer/'main.go').write_text('''package main
import (
 "context"
 "crypto/sha1"
 "encoding/base64"
 "fmt"
 "io"
 "net/http"
 "net/http/httptest"
 "os"
 "strings"
 "time"
 '''+'\n '.join(imports)+'''
)
func must(err error) { if err!=nil {panic(err)} }
func main() {
 for _, name := range []string{"HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","http_proxy","https_proxy","all_proxy","CODEX_CA_CERTIFICATE","SSL_CERT_FILE"} { _=os.Unsetenv(name) }
 server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
  if r.URL.Path!="/ws" { _,_=io.WriteString(w,"native-http"); return }
  socket, stream, err := w.(http.Hijacker).Hijack(); if err!=nil {return}; defer socket.Close()
  _=socket.SetDeadline(time.Now().Add(15*time.Second))
  digest:=sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key")+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
  _,_=fmt.Fprintf(stream,"HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: %s\\r\\n\\r\\n",base64.StdEncoding.EncodeToString(digest[:]))
  if stream.Flush()!=nil {return}
  for {
  var head [2]byte
  if _,err=io.ReadFull(stream,head[:]);err!=nil{return}
  if head[1]&0x80==0 || head[1]&0x7f>=126 {return}
  var mask [4]byte
  if _,err=io.ReadFull(stream,mask[:]);err!=nil{return}
  data:=make([]byte,int(head[1]&0x7f))
  if _,err=io.ReadFull(stream,data);err!=nil{return}
  for i:=range data {data[i]^=mask[i%4]}
  if head[0]==0x89 { _,_=socket.Write(append([]byte{0x8a,byte(len(data))},data...)); continue }
  if head[0]!=0x81 {return}
  _,_=socket.Write(append([]byte{0x81,byte(len(data))},data...))
  return
  }
 }))
 defer server.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
 _=strings.TrimPrefix
 '''+'\n '.join(checks)+'''
}
''')
env=os.environ.copy()
env.update({'GOOS':'linux','GOARCH':args.arch,'CGO_ENABLED':'1','GOWORK':'off','GOTOOLCHAIN':'local',
            'GOMODCACHE':str(work/'module-cache'),'GOPROXY':proxy.as_uri(),'GOSUMDB':'off'})
if args.cc:env['CC']=args.cc
for key in ['CGO_CFLAGS','CGO_CPPFLAGS','CGO_CXXFLAGS','CGO_LDFLAGS','LD_LIBRARY_PATH','LIBRARY_PATH']:
    env.pop(key,None)
runner=shlex.split(args.runner)
run_dir=work/'run';run_dir.mkdir()

def run(command, cwd=consumer):
    print('+',shlex.join(map(str,command)),flush=True)
    subprocess.run(list(map(str,command)),cwd=cwd,env=env,check=True,timeout=180)

for mode in ['mod','vendor']:
    if mode=='vendor':run(['go','mod','vendor'])
    executable=run_dir/('app-'+mode)
    run(['go','build','-mod='+mode,'-o',executable,'.'])
    dynamic=subprocess.check_output(['readelf','-d',executable],text=True)
    assert 'RUNPATH' not in dynamic and 'RPATH' not in dynamic, dynamic
    for forbidden in ['libgocodex','libssl','libcrypto','libubsan']:
        assert forbidden not in dynamic, dynamic
    run([*runner,executable],cwd=run_dir)
# Demonstrate that deployment needs neither module-cache nor native archive files.
# Go deliberately marks downloaded module directories read-only.
for directory, _, _ in os.walk(work/'module-cache'):
    Path(directory).chmod(0o755)
shutil.rmtree(work/'module-cache')
shutil.rmtree(consumer/'vendor')
run([*runner,run_dir/'app-vendor'],cwd=run_dir)
print('PASS:',args.arch,args.packages,'module proxy, vendoring, standalone executable; artifacts in',work)
