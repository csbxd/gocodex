# Normal Go builds use the checked-in archives; only maintainers need Rust.
.PHONY: build test race cgocheck check verify-native submodules prebuild rust-test rust-check consumer-test notices
build:
	go build ./...
test:
	go test -count=1 -timeout=90s ./...
race:
	go test -race -count=1 -timeout=90s ./...
cgocheck:
	GOEXPERIMENT=cgocheck2 go test -count=1 -timeout=90s ./...
check: verify-native
	go vet ./...
verify-native:
	python3 scripts/verify-native.py
submodules:
	git submodule update --init --recursive
prebuild: submodules
	python3 scripts/prebuild.py $(PREBUILD_FLAGS)
rust-test: submodules
	cargo test --locked --lib
rust-check: submodules
	cargo fmt -p gocodex-ffi -- --check
	cargo clippy --locked --all-targets -- -D warnings
consumer-test:
	python3 scripts/smoke-consumer.py $(CONSUMER_FLAGS)
notices: submodules
	python3 scripts/licenses.py
