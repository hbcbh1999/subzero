# Assuming Check is installed via Homebrew on Apple Silicon Mac
INCLUDE_PATH=-I/opt/homebrew/include
LIBRARY_PATH=-L/opt/homebrew/lib
TARGET_DIR=../target
INTROSPECTION_QUERIES_DIR=../introspection
MAC_RELEASE_DIR=$(TARGET_DIR)/release
LINUX_RELEASE_DIR=$(TARGET_DIR)/x86_64-unknown-linux-gnu/release
WINDOWS_RELEASE_DIR=$(TARGET_DIR)/x86_64-pc-windows-gnu/release
VERSION=$(shell grep '^version' Cargo.toml | cut -d '"' -f 2)
DIST_DIR=$(TARGET_DIR)/dist/subzero-ffi-$(VERSION)

CC=gcc
DEBUG_FLAGS=$(INCLUDE_PATH) -I$(TARGET_DIR)/debug -lcheck $(LIBRARY_PATH) -L$(TARGET_DIR)/debug -lsubzero
RELEASE_FLAGS=$(INCLUDE_PATH) -I$(TARGET_DIR)/release -lcheck $(LIBRARY_PATH) -L$(TARGET_DIR)/release -lsubzero

all: test_debug

test_debug: build_debug test_all_debug run_test

test_release: build_release test_all_release run_test

docs: build_release
	# remove old docs from docs/html/ leaving the directory
	rm -rf docs/html/*
	doxygen

run_test:
	./test/test_all

test_all_debug: test/test_all.c
	$(CC) -o test/test_all test/test_all.c $(DEBUG_FLAGS)

test_all_release: test/test_all.c
	$(CC) -o test/test_all test/test_all.c $(RELEASE_FLAGS)

test: test_debug

clean:
	rm -f test/test_all

build_debug:
	cargo build

build_release:
	# build mac
	cargo build --release && \
	install_name_tool -id @rpath/libsubzero.dylib $(TARGET_DIR)/release/libsubzero.dylib

	# build linux
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
	cargo build --release --target x86_64-unknown-linux-gnu

	# build windows
	cargo build --release --target x86_64-pc-windows-gnu

package: build_release
	rm -rf $(TARGET_DIR)/dist && mkdir -p $(DIST_DIR) && \
	cp $(MAC_RELEASE_DIR)/libsubzero.dylib $(DIST_DIR)/libsubzero.dylib && \
	cp $(LINUX_RELEASE_DIR)/libsubzero.so $(DIST_DIR)/libsubzero.so && \
	cp $(WINDOWS_RELEASE_DIR)/subzero.dll $(DIST_DIR)/subzero.dll && \
	cp $(TARGET_DIR)/release/subzero.h $(DIST_DIR)/subzero.h && \
	mkdir -p $(DIST_DIR)/introspection/ && \
	cp $(INTROSPECTION_QUERIES_DIR)/*.sql $(DIST_DIR)/introspection/ && \
	cd $(DIST_DIR)/.. && \
	ln -s subzero-ffi-$(VERSION) subzero-ffi && \
	zip -ry subzero-ffi-$(VERSION).zip subzero-ffi-$(VERSION) subzero-ffi && \
	ln -s subzero-ffi-$(VERSION).zip subzero-ffi-latest.zip



.PHONY: all test_debug test_release test clean build_debug build_release package
