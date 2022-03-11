#!/usr/bin/env bash

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

SQL_ALL_NAMES="SELECT DISTINCT name FROM gas_fee ORDER BY name;"
DIR_OUT="_directory.json"

echo '{' > $DIR_OUT
echo '    "names": [' >> $DIR_OUT
sqlite3 $SQLI_DB "${SQL_ALL_NAMES}" \
    | awk '(NR>1){printf ",\n"} { printf "        \"%s\"",$1; } END {print "\n"}' \
    >> $DIR_OUT
echo '    ]' >> $DIR_OUT
echo '}' >> $DIR_OUT

sqlite3 $SQLI_DB "${SQL_ALL_NAMES}" \
    | while read NAME
    do
        OUT="${NAME}.json"
        echo '{' > $OUT
        SQL="SELECT commit_hash,gas,uncertain,date FROM gas_fee WHERE name = \"${NAME}\" AND commit_hash IS NOT NULL AND icount IS NOT NULL \
            AND date IN ( SELECT MAX(date) FROM gas_fee WHERE name = \"${NAME}\" AND commit_hash IS NOT NULL AND icount IS NOT NULL GROUP BY commit_hash) \
            ORDER BY date ASC;"
        sqlite3 $SQLI_DB "${SQL}" \
        | awk -F"|" '(NR>1){printf ",\n"} {printf "\"%s\": { \"gas\": %s, \"uncertain\": %s, \"date\": \"%s\"}",$1,$2,$3,$4}' \
        >> $OUT;
        echo -e '\n}' >> $OUT
    done