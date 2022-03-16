#!/usr/bin/env bash

# Reads data from Sqlite DB and exports it in the format the static website expects.
# Output directory can be set with the environemnt variable WWW_PATH.

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

SQL_ALL_NAMES="SELECT DISTINCT name FROM gas_fee ORDER BY name;"
SQL_ALL_PROTOCOL_VERSIONS="SELECT DISTINCT protocol_version FROM gas_fee WHERE protocol_version IS NOT NULL ORDER BY protocol_version;"
DIR_OUT="${WWW_PATH}_directory.json"

# Directory of what data is available
echo '{' > $DIR_OUT
echo '    "estimations": [' >> $DIR_OUT
sqlite3 $SQLI_DB "${SQL_ALL_NAMES}" \
    | awk '(NR>1){printf ",\n"} { printf "        \"%s\"",$1; } END {print "\n"}' \
    >> $DIR_OUT
echo '    ],' >> $DIR_OUT
echo '    "protocols": [' >> $DIR_OUT
sqlite3 $SQLI_DB "${SQL_ALL_PROTOCOL_VERSIONS}" \
    | awk '(NR>1){printf ",\n"} { printf "        \"%s\"",$1; } END {print "\n"}' \
    >> $DIR_OUT
echo '    ]' >> $DIR_OUT
echo '}' >> $DIR_OUT

# For each estimation, one JSON file with one line per estimated commit
sqlite3 $SQLI_DB "${SQL_ALL_NAMES}" \
    | while read NAME
    do
        OUT="${WWW_PATH}${NAME}.json"
        echo '{' > $OUT
        SQL="SELECT commit_hash,gas,uncertain,date FROM gas_fee WHERE name = \"${NAME}\" AND commit_hash IS NOT NULL AND icount IS NOT NULL \
            AND date IN ( SELECT MAX(date) FROM gas_fee WHERE name = \"${NAME}\" AND commit_hash IS NOT NULL AND icount IS NOT NULL GROUP BY commit_hash) \
            ORDER BY date ASC;"
        sqlite3 $SQLI_DB "${SQL}" \
        | awk -F"|" '(NR>1){printf ",\n"} {printf "\"%s\": { \"gas\": %s, \"uncertain\": %s, \"date\": \"%s\"}",$1,$2,$3,$4}' \
        >> $OUT;
        echo -e '\n}' >> $OUT
    done

# One file for each protocol versions' parameter values
sqlite3 $SQLI_DB "${SQL_ALL_PROTOCOL_VERSIONS}" \
    | while read PROTOCOL_VERSION
    do
        OUT="${WWW_PATH}_protocol_v${PROTOCOL_VERSION}.json"
        echo '{' > $OUT
        sqlite3 $SQLI_DB "SELECT name,gas FROM gas_fee WHERE protocol_version = \"${PROTOCOL_VERSION}\" ORDER BY protocol_version;" \
            | awk -F"|" '(NR>1){printf ",\n"} {printf "\"%s\": %s",$1,$2 }' \
        >> $OUT;
        echo -e '\n}' >> $OUT
    done