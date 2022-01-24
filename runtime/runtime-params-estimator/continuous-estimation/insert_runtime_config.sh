#!/usr/bin/env bash

# Takes a runtime configuration JSON file and inserts the values into sqlite DB.

if [[ ! -f "$1" ]]; then
    echo 'First argument must be path to a valid runtime config JSON.'
    exit
fi
CONFIG_JSON="$1"

if [[ -z "$2" ]]; then
    echo 'Second argument must be version.'
    exit
fi
VERSION="$2"


if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

while read QUERY NAME;
do
    GAS="`jq "${QUERY}" "${CONFIG_JSON}"`";
    echo "INSERT INTO gas_fee(name,gas,protocol_version) VALUES(\"$NAME\",$GAS,$VERSION);" | sqlite3 ${SQLI_DB};
done < estimations_to_fees.txt