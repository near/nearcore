#!/bin/bash

usage() {
    cat <<-EOF
	Usage:
	    get-validators.sh <socket addr> [<out file>]
	EOF
    exit 1
}

if [ $# -lt 1 ] || [ $# -gt 2 ] ; then
   usage
fi

url=$1
if [ -z $2 ] ; then
    out=$(mktemp '/tmp/validators.txt.XXX')
else
    out=$2
fi

echo "writing results to $out"

tmp=$(mktemp)
curl --silent --request "POST" --header "Content-Type: application/json" \
     --data '{"jsonrpc": "2.0", "id":"dontcare", "method": "validators", "params": {"latest": null}}' -o "$tmp" "$url"

err=$(jq '.error' $tmp)

if [ "$err" != "null" ] ; then
    echo "bad response"
    cat $tmp
    rm $tmp
    exit 1
fi

jq '.result.current_validators[].account_id // empty' $tmp > $out
jq '.result.next_validators[].account_id // empty' $tmp >> $out

rm $tmp
