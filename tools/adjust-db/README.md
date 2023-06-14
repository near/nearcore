# adjust-db tool
This is a tool that should only be used for testing purposes.  
It is intended as a collection of commands that perform small db modifications.


## change-db-kind
Changes DbKind of a DB described in config (cold or hot).  
Example usage:
`neard adjust-db change-db-kind --new-kind RPC change-cold`  
In this example we change DbKind of the cold db to RPC (for some reason).  
Notice, that you cannot perform this exact command twice in a row,
because you will not be able to open cold db in the first place.  
If you want to change DbKind of the cold db back, you would have to adjust your config:
- .store.path = `<relative cold path>`
- .cold_store = null
- .archive = false

This way neard would try to open db at cold path as RPC db.  
Then you can call
`neard adjust-db change-db-kind --new-kind Cold change-hot`.
Notice that even though in your mind this db is cold, in your config this db hot, so you have to pass `change-hot`.


