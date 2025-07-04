# Release Forknet Tool

The `release_forknet.py` script is used to run release test scenarios.

## Prerequisites
* GCP access (SRE)
* Infra-ops access (SRE)
* Install gcloud tools locally [link](https://cloud.google.com/sdk/docs/install)
* Understand the basics of forknet from [mirror](mirror.md)

## Usage

1. Go to `pytest` directory.
2. Create the nodes:

```bash
python tests/mocknet/release_forknet.py --unique-id UNIQUE-ID --test-case test2.7 create
```

3. Start the test
```bash
python python tests/mocknet/release_forknet.py --unique-id token --test-case test2.7 start_test --neard-upgrade-binary-url $NEARD_UPGRADE_BINARY_URL
```

4. Stop the test and free the nodes
```bash
python python tests/mocknet/release_forknet.py --unique-id token --test-case test2.7 destroy
```

## Adding testcase
The test case functions will be executed sequentially in the order defined in [release_forknet.py](../release_forknet.py).
To add a new test scenario:

1. Create a new class in `tests/mocknet/release_scenarios/` that inherits from `TestSetup` base class

2. Define required parameters in `__init__`:
   ```python
   def __init__(self, args):
       # Set base binary URL for initial network state
       args.neard_binary_url = 'https://...'
       super().__init__(args)

       # Required parameters:
       self.start_height = 123456789  # Forknet image height
       self.args.start_height = self.start_height
       self.validators = 21  # Total validator count
       self.block_producers = 18  # Number of block producers
       self.epoch_len = 18000  # Epoch length in blocks
       self.genesis_protocol_version = 77  # Protocol version

       # Optional parameters:
       self.has_archival = True  # Enable archival nodes
       self.has_state_dumper = False  # Enable state dumper
       self.regions = None  # List of GCP regions, None for default
   ```

3. Override methods to customize behavior:
   - `amend_configs()` - Modify node configs before start
   - `before_test_setup()` - Run commands before test creation
   - `after_test_start()` - Run commands after test starts

4. Add your class to the list of testcases under [TEST_CASES](../release_scenarios/__init__.py).
