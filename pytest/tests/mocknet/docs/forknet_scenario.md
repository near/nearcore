# Forknet Scenario Tool

The `forknet_scenario.py` script is used to run test scenarios.

## Prerequisites
* GCP access (SRE)
* Infra-ops access (SRE)
* Install gcloud tools locally [link](https://cloud.google.com/sdk/docs/install)
* Understand the basics of forknet from [mirror](mirror.md)

## Usage

1. Go to `pytest` directory.
2. Create the nodes:

```bash
python tests/mocknet/forknet_scenario.py --unique-id UNIQUE-ID --test-case Example create
```

3. Start the test
```bash
python tests/mocknet/forknet_scenario.py --unique-id token --test-case Example start --neard-upgrade-binary-url $NEARD_UPGRADE_BINARY_URL
```

4. Stop the test and free the nodes
```bash
python tests/mocknet/forknet_scenario.py --unique-id token --test-case Example destroy
```

## Adding testcase
The test case functions will be executed sequentially in the order defined in [forknet_scenario.py](../forknet_scenario.py).
To add a new test scenario:

1. Create a new class in `tests/mocknet/forknet_scenarios/` that inherits from `TestSetup` base class

2. Define required parameters in `__init__`:
   ```python
   def __init__(self, args):
      super().__init__(args)

      # Required parameters:
      self.start_height = 123456789  # Forknet image height
      self.args.start_height = self.start_height

      self.node_hardware_config = NodeHardware.SameConfig(
         num_chunk_producer_seats=19, # Number of block producers and chunk producers
         num_chunk_validator_seats=24, # Total number of validator nodes in the network.
      )

      self.epoch_len = 18000  # Epoch length in blocks
      self.genesis_protocol_version = 77  # Protocol version

      # Optional parameters:
      self.has_archival = True  # Enable archival nodes
      self.has_state_dumper = False  # Enable state dumper
      self.regions = None  # List of GCP regions, None for default
      # Set base binary URL for initial network state
      self.neard_binary_url = 'https://...'
   ```

3. Override methods to customize behavior:
   - `before_test_setup()` - Run commands before test creation
   - `amend_configs_before_test_start()` - Modify node configs before start
   - `after_test_start()` - Run commands after test starts
