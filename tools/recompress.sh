#!/bin/sh

set -eu

help() {
	cat <<EOF
Recompress and garbage collect NEAR archival node’s database
usage: $0 [<option> ...]

The script will create

Possible <option>s are:
     --home=<neard-home>                                               [~/.near]
          Location of neard home directory.
     --backup=<backup-dir>                            [<neard-home>/data.backup]
          Location where to store backup of the database.  The directory must
          not exist.  If it’s on a different file system than <neard-home>, it
          requires enough free space to hold copy of the database.
     --no-backup
          Disables creation of backup.  Not recommended unless the database is
          already backed up somewhere.
     --temp=<temp-dir>                                  [<neard-home>/data.temp]
          Temporary directory where new database will be crceated.  The
          directory must not exist and the file system should have enough space
          to hold copy of the database.

     --neard=<neard>                                                     [neard]
          Path to neard executable.
     --ldb=<ldb>                                                           [ldb]
          Path to the ldb executable from rocksdb-tools package.

  -n --dry-run  Only print out commands which would be executed.
     --time     Time all the migration commands.  Makes output more verbose.
EOF
}

err() {
	echo "${0##*/}: $*" >&2
}

_() {

home_dir=$HOME/.near
backup_dir=
temp_dir=
backup=true

dry_run=
time=
neard=neard
ldb=ldb

for arg; do
	case $arg in
	--help|-h)
		help
		exit 0
		;;
	--home=?*)     home_dir=${arg#*=}   ;;
	--backup=?*)   backup_dir=${arg#*=} ;;
	--no-backup)   backup=false         ;;
	--temp=?*)     temp_dir=${arg#*=}   ;;
	--neard=?*)    neard=${arg#*=}      ;;
	--ldb=?*)      ldb=${arg#*=}        ;;
	-n|--dry-run)  dry_run=:            ;;
	--time)        time='time'          ;;
	*)
		err "$arg: unrecognised argument"
		exit 1
	esac
done

data_dir=$home_dir/data
test -n "$backup_dir" || backup_dir=$home_dir/data.backup
test -n "$temp_dir" || temp_dir=$home_dir/data.temp

for bin in "$neard" "$ldb" $time; do
	if ! which "$bin" >/dev/null; then
		err "$bin: command not found"
		err "$bin: use --$bin=<path> to point to it if it’s not on \$PATH"
		exit 1
	fi
done
if ! "$neard" --help |grep -q recompress-storage; then
	err "unsupported version of neard at $neard"
	err "neard 1.26+ release with ‘recompress-storage’ command is required"
	exit 1
fi

if [ ! -d "$data_dir" ]; then
	err "$data_dir: no such directory"
	exit 1
fi
if $backup && [ -e "$backup_dir" ]; then
	err "$backup_dir: already exists"
	exit 1
fi
if [ -e "$temp_dir" ]; then
	err "$temp_dir: already exists"
	exit 1
fi

echo
echo Make sure than neard is currently NOT running.
echo Executing this script while the node is still operating will corrupt the database.
echo
echo Press RETURN to continue or ^C to abort.
echo
# shellcheck disable=SC2162
read _ || exit 0

run() {
	echo "+ $*" >&2
	$dry_run $time "$@"
}

# Create a checkpoint so that we can operate on it witohut
if $backup; then
	echo 'Creating backup...'
	run "$ldb" --db="$data_dir" checkpoint --checkpoint_dir="$backup_dir"
fi

# Get rid of ColPartialChunks.  Data in that column can be recomputed from
# ColChunks.  Normally the node will keep the lasts 5 epochs worth of data in
# that column but for this migration we’re just getting rid of the whole column.
echo 'Garbage collecting ColPartialChunks...'
run "$ldb" --db="$data_dir" drop_column_family col14
run "$ldb" --db="$data_dir" create_column_family col14

# Recompress.  This is the time-consuming part.
echo 'Recompressing...'
run "$neard" --home="$home_dir" recompress-storage --output-dir="$temp_dir"

# Replace old data with new one.
echo 'Replacing old database with new one...'
run rm -rf -- "$data_dir"
run mv -f -- "$temp_dir" "$data_dir"

echo
echo 'Done.'
if $backup; then
	echo "Database backup is stored in $backup_dir"
fi

}

_ "$@"
