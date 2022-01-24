#!/usr/bin/env bash

# Exports a markdown table 3-way comparing the newest estimations and the newest protocol parameters

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

echo "| Cost | Parameter value | Time based estimation | Icount based estimation | P/T | P/I | T/I |"
echo "| --- | --- | --- | --- | --- | --- | --- |"

sqlite3 $SQLI_DB << 'EOF' | awk -F'|' '{printf "| %-36s | %+16s | %+16s | %+16s | %.2f | %.2f | %.2f |\n", $1, $2, $3, $4, $3 ? $2/$3 : 0, $4 ? $2/$4 : 0, $4 ? $3/$4 : 0}'
SELECT t.name,
    p.gas,
    t.gas,
    c.gas
FROM (
        SELECT *
        FROM gas_fee
        WHERE wall_clock_time IS NOT NULL
            AND date IN (
                SELECT MAX(date)
                FROM gas_fee
                WHERE wall_clock_time IS NOT NULL
                GROUP BY name
            )
    ) t
    LEFT JOIN (
        SELECT *
        FROM gas_fee
        WHERE icount IS NOT NULL
            AND date IN (
                SELECT MAX(date)
                FROM gas_fee
                WHERE icount IS NOT NULL
                GROUP BY NAME
            )
    ) c ON t.name = c.name
    LEFT JOIN (
        SELECT *
        FROM gas_fee
        WHERE protocol_version IS NOT NULL
            AND date IN (
                SELECT MAX(date)
                FROM gas_fee
                WHERE protocol_version IS NOT NULL
                GROUP BY NAME
            )
    ) p ON t.name = p.name;
EOF
