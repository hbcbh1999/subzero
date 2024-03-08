JAVA_HOME=/opt/homebrew/opt/openjdk
INCLUDE_PATH=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin -I../target/release/ 
LIBRARY_PATH=-L../target/release/ -lsubzero

build: swig
	mvn compile

test: build
	mvn test -DargLine="-Djava.library.path=."

build_rust:
	cd ../subzero-ffi && cargo build --release

clean:
	mvn clean

package:
	mvn package

swig_clean:
	rm -rf src/main/java/com/subzero/swig && mkdir -p src/main/java/com/subzero/swig

swig_generate: swig_clean
	swig -java -package com.subzero.swig \
		-outdir src/main/java/com/subzero/swig \
		${INCLUDE_PATH} subzero.i
swig_compile: swig_generate
	gcc -c -fpic subzero_wrap.c -o subzero_wrap.o $(INCLUDE_PATH)
swig_link:
	gcc -shared -o libsubzerojni.dylib subzero_wrap.o $(LIBRARY_PATH)

swig: swig_compile swig_link



.PHONY: build test clean package

