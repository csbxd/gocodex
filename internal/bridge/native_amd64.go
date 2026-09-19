//go:build cgo && linux && amd64

package bridge

// The blank import makes go mod vendor retain the matching native archive.
import _ "github.com/csbxd/gocodex/native/linux_amd64"
