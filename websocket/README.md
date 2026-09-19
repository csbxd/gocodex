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
  `WriteControl`、`Close`。握手支持 `http.Header`，包括认证头、cookie 和子协议。
- 支持文本、二进制、Ping、Pong、Close；完整消息缓存在内存。
  文本校验 UTF-8，可设置 `MaxMessageSize` 和 `MaxFrameSize`（0 使用 SDK 默认值）。
- 同一连接可并发读写。数据消息之间串行发送，每条消息默认拆成 32 KiB 的有序分片；
  `WriteFrameSize` 可调整分片大小（最大 1 MiB）。分片不会混入另一条数据消息。
  等待中的控制帧和自动回复在分片边界优先发送，不被整条大消息占住写锁。
  控制帧仍可能等待当前分片完成，或受网络背压影响。
- `Dial` 的 context 仅控制握手；建立连接后，每次读写使用自己的 context。
  **取消正在执行的读写会关闭整个连接**，避免继续使用只传输了一部分的帧。
  调用前已取消的 context 直接返回错误，不执行网络操作。
- 接收到 Ping 或 Close 时由 SDK 自动回复，`ReadMessage` 仍返回该控制消息。
  自动回复需要持续调用 `ReadMessage`；不要对返回的 Ping 再重复发送 Pong。
  正常协议结束返回 `io.EOF`；传输、协议、握手失败返回 `*Error`。
  HTTP 升级被拒绝时返回 `*HandshakeError` 和非 nil 的 `Handshake`，见下文。
- `Close()` 立即释放连接，并唤醒阻塞操作；客户端 `Close()` 关闭其全部连接。
  需要协议关闭握手时，先发送 `CloseMessage` 和 `ClosePayload(1000, "done")`，
  再在有超时的 context 下读取对端 Close，最后调用 `Close()`。
  `ParseClosePayload` 解码关闭状态及原因。
- 默认 `RespectSystemProxy`，支持显式 `ReqwestDefault`；代理环境变量和
  系统设置由 SDK 处理。也支持 `ProxyURL` 指定代理、`NoProxy` 强制直连。
  `LoopbackDirect` 仅适用于 SDK 验证的本机地址。
- TLS 使用 SDK 的系统根证书及 `CODEX_CA_CERTIFICATE` / `SSL_CERT_FILE`；
  自定义 CA 在创建客户端时读取。初始 `ChatGPTCookies` 按客户端配置；
  SDK 允许缓存的 ChatGPT 基础设施 Cookie 按进程共享。
  环境配置应在创建客户端之前设置。
- 整数句柄、Rust 自有缓冲区、panic 捕获和取消令牌保证跨语言资源生命周期。
  Go 与 Rust 的内存相互复制，不保留 Go 指针；无需 Go 回调。

Go SDK 没有第三方 Go 运行时依赖；`github.com/gorilla/websocket` 只用作测试服务端。
测试覆盖握手头、子协议、文本/二进制、分片消息、并发读写、Ping/Pong、关闭帧、
context 取消、握手超时、WSS、自定义 CA、代理 CONNECT 和消息大小限制。

## 指定代理

```go
client, err := codexws.NewClient(codexws.Options{
    ProxyURL: "http://user:password@127.0.0.1:7890",
    HandshakeTimeout: 30 * time.Second,
})
```

支持 `http://`、`https://`、`socks5://`、`socks5h://`，以及 URL 中的用户名密码。
特殊字符使用 `url.UserPassword` 编码。WS 和 WSS 都走指定代理；HTTP/HTTPS 代理使用
CONNECT 隧道。按当前 SDK 的语义，SOCKS5 和 SOCKS5h 的目标域名均交给代理解析。

`ProxyURL` 覆盖系统代理、`ProxyPolicy` 和包括 `NO_PROXY` 在内的环境变量，
不会修改进程环境。不同客户端可以并发使用不同代理。代理失败直接返回错误。
`ProxyURL` 与 `NoProxy`、`LoopbackDirect` 互斥；`NoProxy: true` 可对任意目的地址直连。

## 被拒绝的握手

```go
conn, handshake, err := client.Dial(ctx, url, headers)
if err != nil {
    var rejected *codexws.HandshakeError
    if errors.As(err, &rejected) {
        // handshake == rejected.Handshake
        // handshake.StatusCode: 401 / 403 / 426 / 429 等
        // handshake.Headers: Retry-After、重复响应头等
        // handshake.Body: 已捕获的错误体
        // handshake.BodyTruncated: 错误体是否不完整或完整性无法确定
    }
    return err
}
defer conn.Close()
```

`HandshakeError.Unwrap()` 保留底层 `*Error`，因此原来的 `errors.As` 检查仍可使用。
错误字符串仅包含握手状态，不包含响应体。状态码和响应头可用于 HTTP 回退、鉴权和限流处理。

**错误体是尽力保留的已读数据。** 上游 SDK 在拒绝升级后释放连接，不继续读取错误体。
当响应体分多次到达、超过 `MaxHandshakeBodyBytes`（默认 64 KiB，最大 1 MiB），
或无法确定是否完整时，`BodyTruncated` 为 true；不应把该数据当作完整 JSON。
已捕获的 chunked 数据会解除分块编码，二进制内容保持原样。不额外重发握手请求。
DNS、TLS、代理连接或 context 失败等没有 HTTP 升级响应的情况，`Handshake` 仍为 nil。

## 控制帧

```go
err := conn.WriteControl(ctx, codexws.PingMessage, []byte("keepalive"))
```

`WriteControl` 只接受 Ping、Pong、Close，可与 `WriteMessage` 并发调用；
控制帧有效载荷最多 125 字节。`WriteMessage` 发送控制类型时具有相同优先级。
关闭握手开始后，后续数据写入返回 `Kind == "closing"`；它不会抢先释放连接，
调用方仍可读取对端 Close 状态（例如 1009），最后调用 `Close()`。
取消正在执行的控制写入和取消普通读写一样，会关闭整个连接。

## 维护者说明

`build.rs` 从固定 Codex 子模块编译 WebSocket SDK 源码，仅将私有的
`connect_with_route` 扩展为桥接层可见；其代理、TLS、Cookie 和协议实现保持原样。
SDK 的原始单元测试也随桥接层运行。子模块不需要修改或切换到其他仓库。
预编译来源记录包含该构建脚本和 Cargo 配置的校验和。
