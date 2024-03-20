JAVA_HOME=/opt/homebrew/opt/openjdk
RESOURCES_DIR=src/main/resources
INCLUDE_PATH=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin -I$(RESOURCES_DIR) 
LIBRARY_PATH=-L$(RESOURCES_DIR) -lsubzero
C_LIB_PATH=../target/dist

build: swig
	mvn compile

test: build
	mvn test -DargLine="-Djava.library.path=."

build_rust:
	cd ../subzero-ffi && make package

clean:
	mvn clean

package:
	mvn package

swig_clean:
	rm -rf src/main/java/com/subzero/swig && mkdir -p src/main/java/com/subzero/swig && \
	rm -rf $(RESOURCES_DIR)/subzero-ffi* && mkdir -p $(RESOURCES_DIR)

docs:
	# Generate javadocs and copy to docs directory
	rm -r target/site/apidocs/ && \
	mvn javadoc:javadoc && \
	cp -r target/site/apidocs/ docs/

unzip_clib:
	unzip -o -d $(RESOURCES_DIR) $(C_LIB_PATH)/subzero-ffi-latest.zip && \
	cp $(RESOURCES_DIR)/subzero-ffi/*.* $(RESOURCES_DIR) && \
	mkdir -p $(RESOURCES_DIR)/introspection && \
	cp $(RESOURCES_DIR)/subzero-ffi/introspection/* $(RESOURCES_DIR)/introspection && \
	rm -rf $(RESOURCES_DIR)/subzero-ffi* 
swig_generate: swig_clean unzip_clib
	swig -java -package com.subzero.swig \
		-outdir src/main/java/com/subzero/swig \
		${INCLUDE_PATH} subzero.i
swig_compile: swig_generate
	gcc -c -fpic subzero_wrap.c -o subzero_wrap.o $(INCLUDE_PATH)
swig_link:
	gcc -shared -o $(RESOURCES_DIR)/libsubzerojni.dylib subzero_wrap.o $(LIBRARY_PATH) -Wl,-rpath,@loader_path/. && \
	install_name_tool -id @rpath/libsubzerojni.dylib $(RESOURCES_DIR)/libsubzerojni.dylib && \
	otool -L $(RESOURCES_DIR)/libsubzerojni.dylib

swig: swig_compile swig_link



.PHONY: build test clean package docs swig_clean swig_generate swig_compile swig_link swig

