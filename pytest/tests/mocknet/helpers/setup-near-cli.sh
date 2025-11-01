#!/usr/bin/env bash
# This script is used to setup the near and near-validator CLI for the
# mocknet cluster.

# First argument is the network id.
NETWORK_ID="${1:-mocknet}"
# Second argument is the traffic node address, which will be used to send
# transactions to the network.
TRAFFIC_NODE_ADDR="${2:-127.0.0.1:3030}"

# Check if near CLI is already installed
if [ -x /home/ubuntu/.cargo/bin/near ]; then
    echo "near CLI is already installed at: /home/ubuntu/.cargo/bin/near"
    echo "Version: $(/home/ubuntu/.cargo/bin/near --version)"
else
    echo "Installing near-cli-rs..."
    curl --proto '=https' --tlsv1.2 -LsSf https://github.com/near/near-cli-rs/releases/latest/download/near-cli-rs-installer.sh | sh
fi

# Check if near-validator is already installed
if [ -x /home/ubuntu/.cargo/bin/near-validator ]; then
    echo "near-validator is already installed at: /home/ubuntu/.cargo/bin/near-validator"
    echo "Version: $(/home/ubuntu/.cargo/bin/near-validator --version)"
else
    echo "Installing near-validator..."
    curl --proto '=https' --tlsv1.2 -LsSf https://github.com/near-cli-rs/near-validator-cli-rs/releases/latest/download/near-validator-installer.sh | sh
fi

source $HOME/.cargo/env

# Configure near-cli by overwriting the config file with desired content
CONFIG_DIR="$HOME/.config/near-cli"
VALIDATOR_KEY_FILE="$HOME/.near/validator_key.json"
CREDENTIALS_DIR="$HOME/.near-credentials"
CONFIG_FILE="$CONFIG_DIR/config.toml"
mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_FILE" <<EOF
version = "3"
credentials_home_dir = "$CREDENTIALS_DIR"

[network_connection.mocknet]
network_name = "$NETWORK_ID"
rpc_url = "http://$TRAFFIC_NODE_ADDR/"
wallet_url = "http://$TRAFFIC_NODE_ADDR/"
explorer_transaction_url = "http://$TRAFFIC_NODE_ADDR/"
EOF

echo "Wrote near-cli config to $CONFIG_FILE (network_name=$NETWORK_ID, ip=$TRAFFIC_NODE_IP)"

# Load account to legacy keychain.
# We can't use `near account import-account` directly because it always
# requests account id in the interactive mode. `expect` tool would be required
# to automate this.
mkdir -p "$CREDENTIALS_DIR/$NETWORK_ID"

CREDENTIALS_FILE="$CREDENTIALS_DIR/$NETWORK_ID/$(jq -r .account_id $VALIDATOR_KEY_FILE).json"

jq '{
  account_id,
  private_key: .secret_key,
  public_key
}' "$VALIDATOR_KEY_FILE" > $CREDENTIALS_FILE
