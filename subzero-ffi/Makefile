# Assuming Check is installed via Homebrew on Apple Silicon Mac
INCLUDE_PATH=-I/opt/homebrew/include
LIBRARY_PATH=-L/opt/homebrew/lib

CC=gcc
DEBUG_FLAGS=$(INCLUDE_PATH) -I../target/debug -lcheck $(LIBRARY_PATH) -L../target/debug -lsubzero
RELEASE_FLAGS=$(INCLUDE_PATH) -I../target/release -lcheck $(LIBRARY_PATH) -L../target/release -lsubzero

all: test_debug

test_debug: build_debug test_all_debug run_test

test_release: build_release test_all_release run_test

docs: build_release
	doxygen

run_test:
	./test/test_all

test_all_debug: test/test_all.c
	$(CC) -o test/test_all test/test_all.c $(DEBUG_FLAGS)

test_all_release: test/test_all.c
	$(CC) -o test/test_all test/test_all.c $(RELEASE_FLAGS)

test: test_debug

clean:
	rm -f teat/test_all

build_debug:
	cargo build

build_release:
	cargo build --release

.PHONY: all test_debug test_release test clean build_debug build_release
