# httpclient

## 用于标准 http.Client

`Transport` 实现 `http.RoundTripper`，零值可以直接使用，也可用 `NewTransport` 配置：

```go
transport, err := httpclient.NewTransport(httpclient.Options{
    UserAgent: "my-app/1.0",
})
if err != nil { return err }

client := &http.Client{
    Transport: transport,
    Timeout: 30 * time.Second,
}
response, err := client.Get("https://example.com")
if err != nil { return err }
defer response.Body.Close()
body, err := io.ReadAll(response.Body)
```

导入标准库 `net/http`、`io`、`time`，以及 `github.com/csbxd/gocodex/httpclient`。
完整示例：在仓库根目录运行 `go run ./examples/roundtripper -url <URL>`。

- 可并发复用；`RoundTrip` 只执行一次 HTTP 事务，重定向交给 `http.Client.CheckRedirect`，
  Cookie 交给 `http.Client.Jar` 或请求头。`Options.DisableRedirects` 在内部始终启用；
  `Options.ChatGPTCookies` 必须为空。
- 请求体会先完整读入内存，然后关闭；包括失败、context 取消和 `Close` 路径。
  响应体保持流式读取。请求体的 `Close` 必须能中断并发的 `Read`，与 `net/http` 要求相同。
- 支持明文 HTTP 的 `Request.Host` 覆盖、重复头、`ContentLength` 校验、chunked 上传、`Request.Close`、
  context 和旧式 `Request.Cancel`。不会修改请求的 URL、Header 等字段。
  HTTPS 的 Host 必须与 URL 一致；SDK 无法正确覆盖 HTTP/2 authority，适配器会在发送前拒绝该组合。
- `Options.Timeout` 覆盖单次事务的上传缓冲、发送及响应体读取；`http.Client.Timeout`
  还覆盖整条重定向链。取消和超时错误支持 `errors.Is`。
- 普通使用只需关闭每次响应的 `response.Body`。请求句柄和流在 EOF、Body.Close、
  失败或 context 取消时释放；连接池归 Transport 所有，供后续请求复用。
- Transport 不再被引用且其响应体已结束后，由 Go 运行时清理原生客户端，清理时机取决于 GC。
  `Transport.Close()` 是可选的立即释放/终止操作，会中断在途请求并永久关闭 Transport。
  当前没有 `CloseIdleConnections` 方法；`http.Client.CloseIdleConnections()` 对此 Transport 不做操作。
- 当前桥接层不提供流式上传、请求 Trailer、响应 Trailer 值、`Response.TLS` 连接状态
  或 `httptrace` 回调。CONNECT 和协议升级请求会返回错误；WebSocket 使用独立的
  `github.com/csbxd/gocodex/websocket` 包。

## 使用

```go
import (
    "context"
    "io"
    "time"
    codexhttp "github.com/csbxd/gocodex/httpclient"
)

client, err := codexhttp.NewClient(codexhttp.Options{
    Timeout: 30 * time.Second,
    ProxyPolicy: codexhttp.RespectSystemProxy,
})
if err != nil { return err }
defer client.Close()

response, err := client.PostJSON(context.Background(), url, map[string]any{"hello": "world"})
if err != nil { return err }
defer response.Body.Close()
body, err := io.ReadAll(response.Body)
// response.StatusCode、Headers、URL、Protocol、ContentLength 可直接访问。
_ = body
return err
```

在仓库根目录运行完整示例：

```sh
go run ./examples/http -url http://127.0.0.1:8080/
```

## 接口与行为

- `NewClient`、`Do`、`Get`、`PostJSON`、`Close`；客户端支持并发请求。
- `Request` 支持任意 HTTP 方法、重复请求头、二进制 `Body` 或 `JSON`。
  请求体缓存在内存；响应体为 `io.ReadCloser`，收到响应头即可开始读取。
- `ctx` 从请求开始一直有效到响应体 EOF/关闭，取消会中断 Rust 网络操作。
  `Close` 同时取消客户端所有请求；客户端和响应体都必须显式关闭。
- 4xx/5xx 保留为响应状态；网络失败返回 `*Error`，支持 `Timeout()`。
- 默认 `RespectSystemProxy`，也可显式选择 SDK 的 `ReqwestDefault`。
  未指定 `ProxyURL` 或 `NoProxy` 时，沿用 SDK 的系统代理和环境变量规则。
- 自定义 CA 使用 `CODEX_CA_CERTIFICATE`，其次为 `SSL_CERT_FILE`。
  CA 在 SDK 创建具体路由客户端时加载，配置错误可能在首次请求时返回。
- 支持 `DisableRedirects`、`UserAgent`、`ChatGPTCookies`、`TLSBackendFallback`。
  HTTP 和 WebSocket 客户端的显式 Cookie 配置各自独立；
  SDK 允许缓存的 ChatGPT 基础设施 Cookie 按进程共享。
- C ABI 使用整数句柄，不保留 Go 指针；输入复制到 Rust，输出复制回 Go 后
  由 Rust 释放。Rust panic 转为错误；网络任务只运行在桥接层的 Tokio 线程。

Go SDK 本身没有第三方 Go 运行时依赖。测试覆盖流式响应、二进制数据、JSON、
重复头、连接复用、代理、重定向、TLS、自定义 CA、超时、取消和并发关闭。

## 指定代理

`NewClient` 和 `NewTransport` 都支持按客户端指定代理：

```go
transport, err := httpclient.NewTransport(httpclient.Options{
    ProxyURL: "http://127.0.0.1:7890",
})
if err != nil { return err }
client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
```

- 支持 `http://`、`https://`、`socks5://`、`socks5h://`。SOCKS5 使用本地 DNS，
  SOCKS5h 由代理解析目标域名。代理 URL 不接受路径（根路径 `/` 除外）、查询或片段。
- 用户名密码可放在 URL 中，例如 `http://user:password@127.0.0.1:7890`。
  有特殊字符时使用标准库 `url.UserPassword` 构造 URL。
- `ProxyURL` 优先于 `ProxyPolicy`、系统代理和 `HTTP_PROXY` / `HTTPS_PROXY` /
  `ALL_PROXY` / `NO_PROXY`；HTTP 和 HTTPS 请求都使用该代理，连接失败直接返回错误。
- `Options{NoProxy: true}` 强制直连，与 `ProxyURL` 互斥。两者都未设置时保留默认行为。
- 每个客户端独立配置，不修改进程环境变量；并发客户端可以使用不同代理。
- 显式代理和直连模式仍支持自定义 CA，但不能与 `TLSBackendFallback` 同时启用；
  无效配置在构造时返回错误。CA 文件也在构造时加载。

可运行示例：`go run ./examples/roundtripper -url https://example.com -proxy http://127.0.0.1:7890`。
这些选项属于 `httpclient` 包，WebSocket 的代理配置仍使用原有 SDK 规则。

## 生命周期验证

测试覆盖临时 `http.Client` 的响应仍可继续读取、EOF/Close 后解除 Transport 保活、
GC 后关闭原生连接池，以及只关闭响应体时的跨请求连接复用。

在 16 KiB 页的 arm64 主机上通过 QEMU 运行 amd64 强制 GC 测试时，验证命令使用
`-E GODEBUG=madvdontneed=0`；默认内存回收配置下的模拟器异常也能由纯 Go HTTP 程序复现。
这是模拟测试环境的兼容设置，原生 Linux 使用本库不需要设置该变量。
