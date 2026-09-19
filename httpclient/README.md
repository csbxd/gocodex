# httpclient

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
  代理通过 SDK 的系统设置和 `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` /
  `NO_PROXY` 配置；没有另建一套代理实现。
- 自定义 CA 使用 `CODEX_CA_CERTIFICATE`，其次为 `SSL_CERT_FILE`。
  CA 在 SDK 创建具体路由客户端时加载，配置错误可能在首次请求时返回。
- 支持 `DisableRedirects`、`UserAgent`、`ChatGPTCookies`、`TLSBackendFallback`。
  HTTP 和 WebSocket 客户端各自持有 cookie store，不自动共享。
- C ABI 使用整数句柄，不保留 Go 指针；输入复制到 Rust，输出复制回 Go 后
  由 Rust 释放。Rust panic 转为错误；网络任务只运行在桥接层的 Tokio 线程。

Go SDK 本身没有第三方 Go 运行时依赖。测试覆盖流式响应、二进制数据、JSON、
重复头、连接复用、代理、重定向、TLS、自定义 CA、超时、取消和并发关闭。
