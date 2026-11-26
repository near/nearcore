# Mocknet Helper Scripts

This directory contains Python helper scripts to be run on mocknet nodes. They are used in conjunction with mocknet test scenarios and represent common functionality to run on machine with neard.

The `host/` directory holds the scripts that `mirror.py init` uploads to `~/.near/forknet` on each host (`~/.near/target/forknet` on the traffic generator). They install the `neard` systemd user unit, build the test chain state for `new-test`, and start, stop and reset neard. `common.sh` lists the files they keep there.
