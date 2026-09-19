# gocodex

Go 1.27 bindings for the Codex HTTP and WebSocket SDKs.

一个仓库、一个 Go module，提供两个独立包：

```go
import "github.com/csbxd/gocodex/httpclient"
import "github.com/csbxd/gocodex/websocket"
```

两个包共用 `internal/bridge`、Rust C ABI、Tokio 运行时及原生依赖。
每个架构只分发一份 `libgocodex_ffi.a`，支持单独使用任一包或同时使用。
客户端的请求、连接、取消状态和显式配置的 Cookie 各自独立。
SDK 的 ChatGPT 基础设施 Cookie 缓存按进程共享。

## 使用

支持 Linux amd64 / arm64（glibc 2.17+），需要 Go 1.27+ 和正常的 cgo 环境：
有 C 编译器，`CGO_ENABLED=1`。发布后像普通 Go 依赖一样引入：

```sh
go get github.com/csbxd/gocodex/httpclient github.com/csbxd/gocodex/websocket
go build ./...
```

使用方无需 Rust、Cargo、Makefile、Codex 子模块、`go generate` 或 OpenSSL 开发库。
cgo 自动选择随模块分发的预编译静态库；部署时无需携带 SDK `.so`、项目目录或设置库路径。
程序使用 Linux 的 glibc / libgcc 系统运行库，TLS 使用系统 CA 或显式配置的 CA。
支持 `go mod vendor`。跨架构编译按 Go/cgo 的规则使用对应 C 交叉编译器。
不支持 `CGO_ENABLED=0`、Alpine/musl、Windows 和 macOS。

HTTP 示例：

```go
client, err := httpclient.NewClient(httpclient.Options{})
if err != nil { return err }
defer client.Close()

response, err := client.Get(ctx, "https://example.com")
if err != nil { return err }
defer response.Body.Close()
data, err := io.ReadAll(response.Body)
```

WebSocket 示例：

```go
client, err := websocket.NewClient(websocket.Options{})
if err != nil { return err }
defer client.Close()

conn, handshake, err := client.Dial(ctx, "ws://127.0.0.1:8080/ws", nil)
if err != nil { return err }
defer conn.Close()

err = conn.WriteMessage(ctx, websocket.TextMessage, []byte("hello"))
```

完整接口说明见 [HTTP](httpclient/README.md) 和 [WebSocket](websocket/README.md)。
`httpclient.NewTransport(options)` 还提供标准 `http.RoundTripper` 实现：

```go
transport, err := httpclient.NewTransport(httpclient.Options{})
if err != nil { return err }
client := &http.Client{Transport: transport}
```

正常使用只需关闭每次返回的 `response.Body`；连接池跨请求复用，Transport 不要求显式关闭。

HTTP 的 `NewClient` 和 `NewTransport` 均支持 `Options{ProxyURL: "http://127.0.0.1:7890"}`，
可使用 HTTP、HTTPS、SOCKS5/SOCKS5h 代理及用户名密码认证。
显式代理覆盖环境变量；`Options{NoProxy: true}` 强制直连。详见 [指定代理](httpclient/README.md#指定代理)。

WebSocket 同样支持 `ProxyURL` / `NoProxy`，并提供失败握手状态、响应头及已捕获错误体，
以及大消息分片期间优先发送的 `WriteControl`。详见 [WebSocket](websocket/README.md)。

可运行示例位于 `examples/http`、`examples/roundtripper`、`examples/echo`。

## 目录

```text
gocodex/
├── go.mod
├── httpclient/        # HTTP 公共 API
├── websocket/         # WebSocket 公共 API
├── internal/bridge/   # 唯一的 cgo 入口和 C 头文件
├── native/
│   ├── linux_arm64/libgocodex_ffi.a
│   └── linux_amd64/libgocodex_ffi.a
├── rust/src/          # 两种传输，共用 ABI 内存管理及 Tokio runtime
├── integration/       # 同一进程内的并发与生命周期测试
├── scripts/           # 维护者构建和发布包验证工具
└── codex/             # Git submodule，仅维护者重新编译时需要
```

Codex 子模块地址为 `https://github.com/openai/codex.git`，固定提交：
`78245b47af2a7aafcabe025828ceecca69db4df1`。

## 维护者构建和验证

普通 Go 构建及测试直接使用仓库内的静态库：

```sh
make build test check race cgocheck
make consumer-test                         # 从模块代理下载后构建、vendor、运行
make consumer-test CONSUMER_FLAGS='--packages httpclient'
make consumer-test CONSUMER_FLAGS='--packages websocket'
```

`consumer-test` 创建一个不包含 Rust 源码、Codex 子模块或 `.so` 的模块分发包。
它验证模块缓存和 vendor 中的构建，以及删除缓存及 vendor 后的可执行程序。
可通过 `--arch`、`--cc`、`--runner` 指定维护者使用的交叉编译器和模拟器。

重新编译原生库需要 Rust/Cargo、目标平台标准库、Zig 0.15.2、Python 3.11+、
Perl、Make，以及 LLVM 的 `ld.lld`、`llvm-objcopy`、`llvm-ar`、`llvm-nm`。

```sh
make prebuild                              # 两个架构；可用 ZIG 指定 Zig 路径
make prebuild PREBUILD_FLAGS='--arch arm64'
make verify-native
make rust-test rust-check
make notices
```

使用 nightly + rust-src 时可传 `--build-std`；本次产物使用 Rust 1.98.1、rust-src
和显式 `RUSTC_BOOTSTRAP=1` 构建。替换静态库后应使用 `go test -a` 避免旧链接缓存。

每份 `.a` 都是实际二进制文件，附带 `build.json` 校验和及来源记录。
原生内部符号被局部化；COMMON 符号保留分配语义并使用私有前缀，防止与宿主冲突。
发布时提交两种架构的 `.a`、来源记录、锁文件及 `THIRD_PARTY_NOTICES.txt`。
第三方许可信息可通过 `make notices` 更新。

## 从原两个项目迁移

| 原 import | 新 import |
| --- | --- |
| `github.com/csbxd/gocodex-http-client` | `github.com/csbxd/gocodex/httpclient` |
| `github.com/csbxd/gocodex-websocket-client` | `github.com/csbxd/gocodex/websocket` |

构造函数、选项、请求和消息 API 保持原有形式。可继续使用 `codexhttp` / `codexws`
作为 import 别名。两个包的 `Error` 现在是同一桥接错误类型的别名，保留 `Kind`、
`Message`、`Timeout()` 和 `errors.As` 用法，原生错误前缀统一为 `gocodex:`；
`Version()` 返回同一桥接库版本。

## 已验证

- Linux arm64：完整 Go 测试、跨传输并发和关闭隔离测试、race、cgocheck2、go vet。
- Linux amd64：交叉构建后通过 QEMU + Debian glibc 2.36 运行完整 Go 测试。
- Rust：ABI/资源管理及 SDK 重定向规则测试、rustfmt、Clippy（`-D warnings`）。
- 两种架构均通过外部模块下载、同时导入两个包、vendor 构建及独立运行验证；
  arm64 还验证了仅导入 `httpclient` 或仅导入 `websocket`。

本地模块代理用于验证发布结构；GitHub 发布仍需提交并推送此仓库。
