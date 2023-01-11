#!/usr/bin/env bash

trap '' HUP
set +o posix


u() {
    printf '\e[4m%s\e[0m' "$*"
}

mysql_shell() {
    docker run --rm -it \
        --network "$docker_network" \
        "$db_type" \
        mysql -u "$1" -p"$1" -h "$docker_name" "$1" -P "$internal_port"
}

postgres_shell() {
    docker run --rm -it \
        --network "$docker_network" \
        "$db_type" \
        bash -c "
            echo '$docker_name:$internal_port:$1:$1:$1' > ~/.pgpass;
            chmod 600 ~/.pgpass;
            psql -U $1 -h $docker_name -p $internal_port $1;
        "
}

get_current_port() {
    docker inspect "$1" | jq -r '.[] | .NetworkSettings.Ports["'$internal_port'/tcp"][0].HostPort'
}

port_active() (
    read -t 1 -n 1 _ < /dev/tcp/127.0.0.1/"$1"
    error=$?

    if (( error == 0 || error > 128 )); then
        exit 0
    else
        exit 1
    fi
) &> /dev/null


declare -A trade_names=(
    [mysql]=MySQL
    [postgres]=PostgreSQL
)

declare -A port_numbers=(
    [mysql]=3306
    [postgres]=5432
)

identifier=
force=
only_kill=
list=
shell=
scripts=$(mktemp -d)
user=
pass=
dbname=
verbose=1
timeout=0

while [ $# -gt 0 ]; do
    case "$1" in
        -t | --type ) db_type="$2"; shift ;;
        -f | --force ) force=1 ;;
        --shell ) shell=1;;
        -k | --kill ) only_kill=1 ;;
        -w) timeout="$2"; shift ;;
        -s|--scripts) scripts="$2" ; shift ;;
        -u|--user) user="$2" ; shift ;;
        -p|--pass) pass="$2" ; shift ;;
        -d|--db) dbname="$2" ; shift ;;
        -q|--quiet) verbose=0 ;;
        -h | --help )
            cat <<HELP
USAGE
    $( basename "$0" ) <-t|--type postgres|mysql> [-s|--scripts dir] [-u|--user user] [-p|--pass password] [-d|--db name] [-w timeout] [-f|--force] [--shell] $( u 'IDENTIFIER' )
    $( basename "$0" ) [-k|--kill] $( u 'IDENTIFIER' )
    $( basename "$0" ) [-h|--help]

DESCRIPTION
    Creates an emphemeral $db_trade_name database with
    Docker using $( u "IDENTIFIER" ) as the root password, main
    username, their password, and also the primary
    database name. $( u 'IDENTIFIER' ) can't start or end
    with dashes, but overall can include alphanumberic
    characters, underscores, and dashes.
HELP
            exit 0
            ;;
        -* )
            echo "error: no such arg '$1'" >&2
            exit 1
            ;;
        * )
            identifier=$1
            ;;
    esac
    shift
done

if [[ -z $identifier ]]; then
    echo "error: missing required paramete IDENTIFIER" >&2
    exit 1
elif [[ ! $identifier =~ ^[a-z0-9_]+([a-z0-9_-]+[a-z0-9_]|[a-z0-9_]*)$ ]]; then
    echo "error: identifier contains illegal characters" >&2
    exit 1
fi

docker_name=$identifier
docker_network=ephemeral-tmp-db-network
docker network create "$docker_network" &> /dev/null

if docker inspect "$docker_name" &> /dev/null; then
    if [[ $only_kill || $force ]]; then
        if [ ${timeout:-1} -gt 0 ]; then
            sleep ${timeout}
        fi
        if docker rm -f "$docker_name" &> /dev/null; then
            echo "Removed existing container successfully"

            [[ $only_kill ]] && exit
        else
            echo "Failed to remove existing container '$docker_name'" >&2
            exit 1
        fi
    elif [[ $shell ]]; then
        current_port=$( get_current_port "$docker_name" )

        if ! port_active "$current_port"; then
            echo "shell: waiting for database to reach a ready state..."

            # Wait for the server to get ready
            while ! port_active "$current_port"; do
                sleep 1
            done
        fi

        echo "Connecting to '$docker_name':$current_port..."
        "${db_type}_shell" "$identifier"
        exit
    else
        echo "Container '$docker_name' already exists... abort" >&2
        exit 1
    fi
elif [[ $only_kill ]]; then
    echo "No container '$docker_name' to kill"
    exit
fi


db_trade_name=${trade_names[$db_type]}
user=${user:-$identifier}
pass=${pass:-$identifier}
dbname=${dbname:-$identifier}

if [[ -z $db_trade_name ]]; then
    echo "error: unknown DB type '$db_type'" >&2
    exit 1
fi

declare -i internal_port=${port_numbers[$db_type]}
declare -i host_port=$internal_port
while port_active "$host_port"; do
    host_port+=1
done

declare -A mysql_env_vars=(
    [root_pass]=MYSQL_ROOT_PASSWORD
    [user]=MYSQL_USER
    [pass]=MYSQL_PASSWORD
    [db]=MYSQL_DATABASE
    [scripts_dir]=/docker-entrypoint-initdb.d
    [connection_string]="mysql://$user:$pass@127.0.0.1:$host_port/$dbname"
)

declare -A postgres_env_vars=(
    [root_pass]=__NO_ROOT_PASSWORD_TO_SET
    [user]=POSTGRES_USER
    [pass]=POSTGRES_PASSWORD
    [db]=POSTGRES_DB
    [scripts_dir]=/docker-entrypoint-initdb.d
    [connection_string]="postgresql://$user:$pass@127.0.0.1:$host_port/$dbname"
)

declare -n env_vars=${db_type}_env_vars

docker run --rm -d \
    -e ${env_vars[root_pass]}="$identifier" \
    -e ${env_vars[user]}="$user" \
    -e ${env_vars[pass]}="$pass" \
    -e ${env_vars[db]}="$dbname" \
    -p $host_port:$internal_port \
    -v "$scripts":${env_vars[scripts_dir]} \
    --network "$docker_network" \
    --name "$docker_name" \
    "$db_type" >/dev/null  && {
    [[ $verbose -eq 1 ]] && echo "Created new ephemeral $db_trade_name database successfully"

    # Wait for the server get ready
    while ! port_active "$host_port"; do
        #echo "Waiting for $db_trade_name database to start..."
        sleep 1
    done

    connection_string=${env_vars[connection_string]}
    [[ $verbose -eq 1 ]] && echo -n "Connection string: "
    echo $connection_string

    if [ ${timeout:-1} -gt 0 ]; then
        [[ $verbose -eq 1 ]] && echo "Waiting $timeout seconds before killing..."
        nice -n 19  $0 -w $timeout -k $identifier > /dev/null 2>&1 &
    fi

    if [[ $shell ]]; then
        [[ $verbose -eq 1 ]] && echo "shell: waiting for database to reach a ready state..."

        "${db_type}_shell" "$identifier"
    fi

    exit
}

echo "Failed to create $db_trade_name database '$docker_name':$host_port..." >&2
exit 1
