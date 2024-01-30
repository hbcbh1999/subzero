#! /bin/bash
#
# Copyright 2018-2022 ZomboDB, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cd subzero-pg

PGVER=$1
IMAGE=$2
PGX_VERSION=$3
ARCH=$(uname -m)

if [ "x${PGVER}" == "x" ] || [ "x${IMAGE}" == "x" ] ; then
	echo 'usage:  ./package.sh <PGVER> <image>'
	exit 1
fi

if [[ ${IMAGE} == *"amazonlinux"* ]] ||[[ ${IMAGE} == *"fedora"* ]] || [[ ${IMAGE} == *"centos"* ]]; then
	PKG_FORMAT=rpm
elif [[ ${IMAGE} == *"alpine"* ]]; then
	PKG_FORMAT=apk
else
	PKG_FORMAT=deb
fi

set -x

OSNAME=$(echo ${IMAGE} | cut -f3-4 -d-)
VERSION=$(cat subzero_pgx.control | grep default_version | cut -f2 -d\')
if [ "${VERSION}" == "@CARGO_VERSION@" ]; then
	VERSION=$(cat Cargo.toml | grep ^version | cut -f2 -d\")
fi


PG_CONFIG_DIR=$(dirname $(grep ${PGVER} ~/.pgx/config.toml | cut -f2 -d= | cut -f2 -d\"))
export PATH=${PG_CONFIG_DIR}:${PATH}

#
# update Rust to the latest version
#
whoami
pwd
ls -la
rustup update || exit 1

#
# ensure cargo-pgx is the correct version and compiled with this Rust version
#
cargo install cargo-pgx --version $PGX_VERSION

#
# build the extension
#
cargo pgx package --profile artifacts || exit $?

#
# cd into the package directory
#
ARTIFACTDIR=/artifacts
BUILDDIR=/build/target/artifacts/subzero_pgx-pg${PGVER}
cd ${BUILDDIR} || exit $?

# strip the binaries to make them smaller
find ./ -name "*.so" -exec strip {} \;

#
# then use 'fpm' to build either a .deb, .rpm or .apk
#

## hack for when we installed ruby via rvm.  if it doesn't work we don't care
source ~/.rvm/scripts/rvm

if [ "${PKG_FORMAT}" == "deb" ]; then
	fpm \
		-s dir \
		-t deb \
		-n subzero-${PGVER} \
		-v ${VERSION} \
		--deb-no-default-config-files \
		-p ${ARTIFACTDIR}/subzero_${OSNAME}_pg${PGVER}-${VERSION}_${ARCH}.deb \
		-a ${ARCH} \
		. || exit 1

elif [ "${PKG_FORMAT}" == "rpm" ]; then
	fpm \
		-s dir \
		-t rpm \
		-n subzero-${PGVER} \
		-v ${VERSION} \
		--rpm-os linux \
		-p ${ARTIFACTDIR}/subzero_${OSNAME}_pg${PGVER}-${VERSION}_1.${ARCH}.rpm \
		-a ${ARCH} \
		. || exit 1

elif [ "${PKG_FORMAT}" == "apk" ]; then
	fpm \
		-s dir \
		-t apk \
		-n subzero-${PGVER} \
		-v ${VERSION} \
		-p ${ARTIFACTDIR}/subzero_${OSNAME}_pg${PGVER}-${VERSION}.${ARCH}.apk \
		-a ${ARCH} \
		. \
		|| exit 1

else
	echo Unrecognized value for PKG_FORMAT:  ${PKG_FORMAT}
	exit 1
fi

