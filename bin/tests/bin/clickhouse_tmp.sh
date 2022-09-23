#!/bin/sh
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

usage() {
	>&2 echo "release: ${release}"
	>&2 echo "usage: clickhouse_tmp [-k] [-t [-p port]] [-w timeout] [-o extra-options] [-d datadir]"
	exit 1
}

trap 'printf "$0: exit code $? on line $LINENO\n" >&2; exit 1' ERR \
	2> /dev/null || exec bash $0 "$@"
trap '' HUP
set +o posix

USER_OPTS=""
SUPERUSER=$(whoami)

>/dev/null getopt ktp:w:o:d:u: "$@" || usage
while [ $# -gt 0 ]; do
	case "$1" in
		-k) KEEP=$1 ;;
		-t) LISTENTO="127.0.0.1" ;;
		-p) CHPORT="$2"; shift ;;
		-w) TIMEOUT="$2"; shift ;;
		-o) USER_OPTS="$2"; shift ;;
		-d) TD="$2"; shift ;;
		-u) SUPERUSER="$2"; shift ;;
		 *) CMD=$1 ;;
	esac
	shift
done


# initdb -V > /dev/null || exit 1
# CHVER=$(pg_ctl -V | awk '{print $4}')

[ -n "$LISTENTO" ] && [ -z "$CHPORT" ] && {
	#CHPORT="$(getsocket)"
	CHPORT="$(comm -23 <(seq 49152 65535 | sort) <(netstat -na  -p tcp | awk '/tcp/{print $4}' | sed 's/\.\([0-9]*\)$/:\1/' | cut -d':' -f2 | sort -u) | shuf | head -n 1)"
}

case ${CMD:-start} in
initdb)
	[ -z $TD ] || mkdir -p $TD
	[ -z $TD ] && TD="$(mktemp -d ${SYSTMP:-/tmp}/ephemeralch.XXXXXX)"
	# initdb --nosync -D $TD/$CHVER --no-locale --encoding=UTF8 -A trust  -U $SUPERUSER > $TD/initdb.out
	# cat <<-EOF >> $TD/$CHVER/postgresql.conf
	#     # log_statement = 'all'
	#     # log_min_messages = debug1
	#     unix_socket_directories = '$TD'
	#     listen_addresses = ''
	#     shared_buffers = 12MB
	#     fsync = off
	#     synchronous_commit = off
	#     full_page_writes = off
	#     log_min_duration_statement = 0
	#     log_connections = on
	#     log_disconnections = on
	# EOF
	touch $TD/NEW
	echo $TD
	;;
start)
	# 1. Find a temporary database directory owned by the current user
	# 2. Create a new datadir if nothing was found
	### 3. Launch a background task to create a datadir for future invocations
	if [ -z $TD ]; then
		for d in $(ls -d ${SYSTMP:-/tmp}/ephemeralch.*/cores 2> /dev/null); do
			td=$(dirname "$d")
			test -O $td/NEW && rm $td/NEW 2> /dev/null && { TD=$td; break; }
		done
		[ -z $TD ] && { TD=$($0 initdb -u $SUPERUSER); rm $TD/NEW; }
		# nice -n 19 $0 initdb > /dev/null &
	else
		[ -O $TD/cores ] || TD=$($0 initdb -d $TD  -u $SUPERUSER)
	fi
	if [ ${TIMEOUT:-1} -gt 0 ]; then
		nice -n 19 $0 $KEEP -w ${TIMEOUT:-60} -d $TD -p ${CHPORT:-8123} stop > $TD/stop.log 2>&1 &
	fi
	[ -n "$CHPORT" ] && OPTS="--http_port=$CHPORT --default_database=public --tcp_port= --mysql_port= --postgresql_port= --users_config=$SCRIPT_DIR/users.xml"
	[ -n "$USER_OPTS" ] && OPTS="$OPTS $USER_OPTS"
	clickhouse-server start --daemon --log-file=$TD/ch.log -- $OPTS $USER_OPTS
	sleep 3
	CHHOST=$TD
	export CHPORT CHHOST
	if [ -n "$CHPORT" ]; then
		url="http://default:default@$LISTENTO:$CHPORT/"
	else
		url="postgresql:///test?host=$(echo $CHHOST | sed 's:/:%2F:g')&user=default&password=default"
	fi
	[ -t 1 ] && echo "$url" || echo -n "$url"
	;;
stop)
	[ ! -f $TD/cores/status ] || {
		>&2 echo "Please specify a ClickHouse data directory using -d "

		exit 1
	}
	[ "$KEEP" == "" ] && trap "rm -r $TD" EXIT
	CHHOST=$TD
	export CHHOST CHPORT
	sleep ${TIMEOUT:-5}
	PID=$(cat $TD/cores/status | awk '/PID:/ {print $2}')
	kill $PID
	sleep 10
	;;
selftest)
	export SYSTMP=$(mktemp -d /tmp/ephemeralch-selftest.XXXXXX)
	trap "rm -r $SYSTMP" EXIT
	printf "Running: "
	printf "initdb "; dir=$($0 initdb)
	printf "start " ; url=$($0 -w 3 -o '-c log_temp_files=100' start)
	printf "psql "  ; [ "$(psql --no-psqlrc -At -c 'select 5' $url)" == "5" ]
	printf "stop "  ; sleep 10
	printf "verify "; ! [ -d dir ]
	echo; echo "OK"
	;;
*)
	usage
	;;
esac

