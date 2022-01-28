BEGIN{FS="[][ ]+"} # Split on ' ', ']', and '['
/ gas / {
    printf "INSERT INTO gas_fee(name,gas,icount,io_read,io_write,uncertain,commit_hash) VALUES(\"%s\",%s,%s,%s,%s,%d,\"%s\")\n",
        $1,
        $2,
        substr($4,1,length($7)-1), # remove i
        substr($5,1,length($5)-1), # remove r
        substr($6,1,length($6)-1), # remove w
        /UNCERTAIN/?1:0,
        git_hash                   # Must be set from outside or will be left empty
}