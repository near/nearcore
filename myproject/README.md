<br />
<br />

<p>
<img src="https://nearprotocol.com/wp-content/themes/near-19/assets/img/logo.svg?t=1553011311" width="240">
</p>

<br />
<br />

## Template for NEAR dapps
### Requirements
##### IMPORTANT: Make sure you have the latest version of NEAR Shell and Node Version > 10.x 
1. node and npm
2. near shell
install with 
```
npm i -g near-shell
```
3.(optional) install yarn to build
```
npm i -g yarn
```
### To run on testnet
Step 1: Create account for the contract and deploy the contract.
In the terminal
```
near login
```
click the link and create your own contract ID

Step 2:
modify src/config.js line that sets the contractName. Set it to id from step 1.
```
const CONTRACT_NAME = "contractId"; /* TODO: fill this in! */
```

Step 3:
Finally, run the command in your terminal.
```
npm install
npm run(yarn) prestart
npm run(yarn) start
```
The server that starts is for static assets and by default serves them to localhost:5000. Navigate there in your browser to see the app running!
