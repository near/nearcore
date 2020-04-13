const shell = require('shelljs');

shell.fatal = true;

shell.cd('contract');
shell.exec('cargo build --target wasm32-unknown-unknown --release');