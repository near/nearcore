BEGIN{FS="[][ ]+"} # Split on ' ', ']', and '['
/ gas / {
    printf "INSERT INTO gas_fee(name,gas,wall_clock_time,uncertain,commit_hash) VALUES(\"%s\",%s,\"%s\",%d,\"%s\");\n",
        $1,
        $2,
        $4, # Time is shown in ns / us / ms. For now, this is included in the stored string.
        /UNCERTAIN/?1:0,
        git_hash                   # Must be set from outside or will be left empty
}