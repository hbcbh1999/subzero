CURRENT_DIR=$(shell pwd)
RESOURCES_DIR=src/main/resources
# JAVA distirbutions were doenloaded from https://adoptium.net/
JDK_VERSION=21.0.2+13
JAVA_HOME_MAC=mac-jdk-$(JDK_VERSION)/Contents/Home
INCLUDE_PATH_MAC=-I$(JAVA_HOME_MAC)/include -I$(JAVA_HOME_MAC)/include/darwin -I$(RESOURCES_DIR)
JAVA_HOME_LINUX=linux-jdk-$(JDK_VERSION)
INCLUDE_PATH_LINUX=-I$(RESOURCES_DIR) -I$(JAVA_HOME_LINUX)/include -I$(JAVA_HOME_LINUX)/include/linux
JAVA_HOME_WINDOWS=windows-jdk-$(JDK_VERSION)
INCLUDE_PATH_WINDOWS=-I$(RESOURCES_DIR) -I$(JAVA_HOME_WINDOWS)/include -I$(JAVA_HOME_WINDOWS)/include/win32


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
	rm -f ./*.o && \
	rm -rf src/main/java/cloud/subzero/swig && mkdir -p src/main/java/cloud/subzero/swig && \
	rm -rf $(RESOURCES_DIR)/subzero-ffi* && mkdir -p $(RESOURCES_DIR)

docs:
# Generate javadocs and copy to docs directory
	rm -rf target/site/apidocs/ && \
	mvn javadoc:javadoc && \
	cp -r target/site/apidocs/ docs/

unzip_clib:
	unzip -o -d $(RESOURCES_DIR) $(C_LIB_PATH)/subzero-ffi-latest.zip && \
	cp $(RESOURCES_DIR)/subzero-ffi/*.* $(RESOURCES_DIR) && \
	mkdir -p $(RESOURCES_DIR)/introspection && \
	cp $(RESOURCES_DIR)/subzero-ffi/introspection/* $(RESOURCES_DIR)/introspection && \
	rm -rf $(RESOURCES_DIR)/subzero-ffi* 

swig_generate: swig_clean unzip_clib
	swig -java -package cloud.subzero.swig \
		-outdir src/main/java/cloud/subzero/swig \
		-I$(RESOURCES_DIR) subzero.i

swig_compile: swig_generate
# compile for linux
	./dockcross-linux-x64 bash -c '"$$CC" -c -fpic subzero_wrap.c -o subzero_wrap_linux.o '"$(INCLUDE_PATH_LINUX)"

# compile for windows
	./dockcross-windows-shared-x64 bash -c '"$$CC" -c -fpic subzero_wrap.c -o subzero_wrap_windows.o '"$(INCLUDE_PATH_WINDOWS)"

# compile for mac
	gcc -c -fpic subzero_wrap.c -o subzero_wrap_mac.o $(INCLUDE_PATH_MAC)

swig_link:
# link for linux
	./dockcross-linux-x64 bash -c '"$$CC" -shared -o $(RESOURCES_DIR)/libsubzerojni.so subzero_wrap_linux.o '"$(LIBRARY_PATH)"' -Wl,-rpath=\$$ORIGIN'
	patchelf --set-rpath '$ORIGIN' $(RESOURCES_DIR)/libsubzerojni.so

# link for windows
	./dockcross-windows-shared-x64 bash -c '"$$CC" -shared -o $(RESOURCES_DIR)/subzerojni.dll subzero_wrap_windows.o '"$(LIBRARY_PATH)"' -Wl,--add-stdcall-alias'

# link for mac
	gcc -shared -o $(RESOURCES_DIR)/libsubzerojni.dylib subzero_wrap_mac.o $(LIBRARY_PATH) -Wl,-rpath,@loader_path/. && \
	install_name_tool -id @rpath/libsubzerojni.dylib $(RESOURCES_DIR)/libsubzerojni.dylib && \
	otool -L $(RESOURCES_DIR)/libsubzerojni.dylib

swig: swig_compile swig_link

deploy: build_rust build
	mvn deploy


.PHONY: build test clean package docs swig_clean swig_generate swig_compile swig_link swig deploy

