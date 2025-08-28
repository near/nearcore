#!/usr/bin/env bash

# Check if near CLI is already installed
if command -v near &> /dev/null; then
    echo "near CLI is already installed at: $(which near)"
    echo "Version: $(near --version)"
else
    echo "Installing near-cli-rs..."
    curl --proto '=https' --tlsv1.2 -LsSf https://github.com/near/near-cli-rs/releases/latest/download/near-cli-rs-installer.sh | sh
fi

# Check if near-validator is already installed
if command -v near-validator &> /dev/null; then
    echo "near-validator is already installed at: $(which near-validator)"
    echo "Version: $(near-validator --version)"
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
NETWORK_ID="${1:-mocknet}"
TRAFFIC_NODE_ADDR="${2:-127.0.0.1:3030}"
mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_FILE" <<EOF
version = "3"
credentials_home_dir = "/home/ubuntu/.near-credentials"

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
mkdir -p $CREDENTIALS_DIR/$NETWORK_ID

jq -n \
  --arg account_id "$(jq -r .account_id $VALIDATOR_KEY_FILE)" \
  --arg private_key "$(jq -r .secret_key $VALIDATOR_KEY_FILE)" \
  --arg public_key "$(jq -r .public_key $VALIDATOR_KEY_FILE)" \
  '{
     account_id: $account_id,
     private_key: $private_key,
     public_key: $public_key
   }' \
  > $CREDENTIALS_DIR/$NETWORK_ID/$(jq -r .account_id $VALIDATOR_KEY_FILE).json
