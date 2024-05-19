
.PHONY: build
build:
	RUSTFLAGS="-C target-cpu=native" cargo build
