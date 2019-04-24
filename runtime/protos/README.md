## Development
`.proto` files are under the `protos` directory. To add/delete protos, one also needs to change the include macro at the beginning of `src/lib.rs`. 

For example, if `example.proto` is added to `protos`, `include!(concat!(env!("OUT_DIR"), "/example.rs"))` needs to be added to `src/lib.rs`.