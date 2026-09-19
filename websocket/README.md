# websocket

## 使用

```go
import (
    "context"
    "time"
    codexws "github.com/csbxd/gocodex/websocket"
)

client, err := codexws.NewClient(codexws.Options{
    HandshakeTimeout: 10 * time.Second,
    TCPNoDelay: true,
})
if err != nil { return err }
defer client.Close()

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
conn, handshake, err := client.Dial(ctx, "ws://127.0.0.1:8080/ws", nil)
if err != nil { return err }
defer conn.Close()
_ = handshake // StatusCode 和 Headers

if err := conn.WriteMessage(ctx, codexws.TextMessage, []byte("hello")); err != nil {
    return err
}
kind, data, err := conn.ReadMessage(ctx)
_, _ = kind, data
return err
```

在仓库根目录运行 echo 客户端示例：

```sh
go run ./examples/echo -url ws://127.0.0.1:8080/ws -message hello
```

## 接口与行为

- 客户端：`NewClient`、`Dial`、`Close`；连接：`ReadMessage`、`WriteMessage`、
  `Close`。握手支持 `http.Header`，包括认证头、cookie 和子协议。
- 支持文本、二进制、Ping、Pong、Close；完整消息缓存在内存。
  文本校验 UTF-8，可设置 `MaxMessageSize` 和 `MaxFrameSize`（0 使用 SDK 默认值）。
- 同一连接可并发读写，Rust 分别串行化读方向和写方向。
- `Dial` 的 context 仅控制握手；建立连接后，每次读写使用自己的 context。
  **取消正在执行的读写会关闭整个连接**，避免继续使用只传输了一部分的帧。
  调用前已取消的 context 直接返回错误，不执行网络操作。
- 接收到 Ping 或 Close 时由 SDK 自动回复，`ReadMessage` 仍返回该控制消息。
  正常协议结束返回 `io.EOF`；传输、协议、握手失败返回 `*Error`。
  握手被拒绝时返回错误，当前 Go 接口不返回失败握手的响应头或响应体。
- `Close()` 立即释放连接，并唤醒阻塞操作；客户端 `Close()` 关闭其全部连接。
  需要协议关闭握手时，先发送 `CloseMessage` 和 `ClosePayload(1000, "done")`，
  再在有超时的 context 下读取对端 Close，最后调用 `Close()`。
  `ParseClosePayload` 解码关闭状态及原因。
- 默认 `RespectSystemProxy`，支持显式 `ReqwestDefault`；代理环境变量和
  系统设置由 SDK 处理。`LoopbackDirect` 仅适用于 SDK 验证的本机地址。
- TLS 使用 SDK 的系统根证书及 `CODEX_CA_CERTIFICATE` / `SSL_CERT_FILE`；
  自定义 CA 在创建客户端时读取。支持初始 `ChatGPTCookies`，与 HTTP 客户端的
  cookie store 不自动共享。环境配置应在创建客户端之前设置。
- 整数句柄、Rust 自有缓冲区、panic 捕获和取消令牌保证跨语言资源生命周期。
  Go 与 Rust 的内存相互复制，不保留 Go 指针；无需 Go 回调。

Go SDK 没有第三方 Go 运行时依赖；`github.com/gorilla/websocket` 只用作测试服务端。
测试覆盖握手头、子协议、文本/二进制、分片消息、并发读写、Ping/Pong、关闭帧、
context 取消、握手超时、WSS、自定义 CA、代理 CONNECT 和消息大小限制。
