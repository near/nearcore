<br />
<br />

<p align="center">
<img src="docs/src/images/logo.svg" width="240">
</p>

<br />
<br />


## Reference implementation of NEAR Protocol

![Buildkite](https://img.shields.io/buildkite/0eae07525f8e44a19b48fa937813e2c21ee04aa351361cd851)
![Stable Status][stable-release]
![Prerelease Status][prerelease]
[![codecov][codecov-badge]][codecov-url]
[![Discord chat][discord-badge]][discord-url]
[![Telegram Group][telegram-badge]][telegram-url]

[stable-release]: https://img.shields.io/github/v/release/nearprotocol/nearcore?label=stable
[prerelease]: https://img.shields.io/github/v/release/nearprotocol/nearcore?include_prereleases&label=prerelease
[ci-badge-master]: https://badge.buildkite.com/a81147cb62c585cc434459eedd1d25e521453120ead9ee6c64.svg?branch=master
[ci-url]: https://buildkite.com/nearprotocol/nearcore
[codecov-badge]: https://codecov.io/gh/nearprotocol/nearcore/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/nearprotocol/nearcore
[discord-badge]: https://img.shields.io/discord/490367152054992913.svg
[discord-url]: https://near.chat
[telegram-badge]: https://cdn.jsdelivr.net/gh/Patrolavia/telegram-badge@8fe3382b3fd3a1c533ba270e608035a27e430c2e/chat.svg
[telegram-url]: https://t.me/cryptonear

## About NEAR

NEAR's purpose is to enable community-driven innovation to benefit people around the world.

To achieve this purpose, *NEAR* provides a developer platform where developers and entrepreneurs can create apps that put users back in control of their data and assets, which is the foundation of ["Open Web" movement][open-web-url].

One of the components of *NEAR* is the NEAR Protocol, an infrastructure for server-less applications and smart contracts powered by a blockchain.
NEAR Protocol is built to deliver usability and scalability of modern PaaS like Firebase at fraction of the prices that blockchains like Ethereum charge.

Overall, *NEAR* provides a wide range of tools for developers to easily build applications:
 - [JS Client library][js-api] to connect to NEAR Protocol from your applications.
 - [Rust][rust-sdk] and [AssemblyScript][as-sdk] SDKs to write smart contracts and stateful server-less functions.
 - [Numerous examples][examples-url] with links to hack on them right inside your browser.
 - [Lots of documentation][docs-url], with [Tutorials][tutorials-url] and [API docs][api-docs-url].

[open-web-url]: https://techcrunch.com/2016/04/10/1301496/
[js-api]: https://github.com/near/near-api-js
[rust-sdk]: https://github.com/near/near-sdk-rs
[as-sdk]: https://github.com/near/near-sdk-as
[examples-url]: https://near.dev
[docs-url]: https://docs.near.org
[tutorials-url]: https://docs.near.org/tutorials/welcome
[api-docs-url]: https://docs.near.org/api/rpc/introduction

## Join the Network

The easiest way to join the network, is by using the `nearup` command, which you can install as follows:

```bash
pip3 install --user nearup
```

You can join all the active networks:
* mainnet: `nearup run mainnet`
* testnet: `nearup run testnet`
* betanet: `nearup run betanet`

Check the `nearup` repository for [more details](https://github.com/near/nearup) how to run with or without docker.

To learn how to become validator, checkout [documentation](https://docs.near.org/docs/develop/node/validator/staking-and-delegation).

## Contributing

The workflow and details of setup to contribute are described in [CONTRIBUTING.md](CONTRIBUTING.md), and security policy is described in [SECURITY.md](SECURITY.md).
To propose new protocol changes or standards use [Specification & Standards repository](https://github.com/nearprotocol/NEPs).

## Getting in Touch

We use Zulip for semi-synchronous technical discussion, feel free to chime in:

https://near.zulipchat.com/

For non-technical discussion and overall direction of the project, see our Discourse forum:

https://gov.near.org
