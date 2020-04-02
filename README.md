<br />
<br />

<p align="center">
<img src="docs/logo.svg" width="240">
</p>

<br />
<br />


## Reference implementation of NEAR Protocol

[![codecov][codecov-badge]][codecov-url]
[![Discord chat][discord-badge]][discord-url]
[![Telegram Group][telegram-badge]][telegram-url]

master | beta | stable
---|---|---|
[![Build Status][ci-badge-master]][ci-url] | [![Build Status][ci-badge-beta]][ci-url] | [![Build Status][ci-badge-stable]][ci-url] 

[ci-badge-master]: https://badge.buildkite.com/a81147cb62c585cc434459eedd1d25e521453120ead9ee6c64.svg
[ci-badge-beta]: https://badge.buildkite.com/a81147cb62c585cc434459eedd1d25e521453120ead9ee6c64.svg?branch=beta
[ci-badge-stable]: https://badge.buildkite.com/a81147cb62c585cc434459eedd1d25e521453120ead9ee6c64.svg?branch=stable
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

One of the components of *NEAR* is NEAR Protocol, an infrastructure for server-less applications and smart contracts powered by blockchain.
NEAR Protocol is built to deliver usability and scalability of modern PaaS like Firebase at fraction of prices that blockchains like Ethereum charge.

*NEAR* overall provides wide range of tools for developers to easily build applications:
 - [JS Client library][js-api] to connect to NEAR Protocol from your applications.
 - [Rust][rust-sdk] and [AssemblyScript][as-sdk] SDKs to write smart contracts and stateful server-less functions.
 - [Numerous examples][examples-url] with links to hack on them right inside your browser.
 - [Lots of documentation][docs-url], with [Tutorials][tutorials-url] and [API docs][api-docs-url].

[open-web-url]: https://techcrunch.com/2016/04/10/1301496/ 
[js-api]: https://github.com/near/near-api-js 
[rust-sdk]: https://github.com/near/near-sdk-rs
[as-sdk]: https://github.com/near/near-sdk-as
[examples-url]: https://near.dev
[docs-url]: http://docs.nearprotocol.com
[tutorials-url]: https://docs.nearprotocol.com/docs/roles/developer/tutorials/introduction
[api-docs-url]: https://docs.nearprotocol.com/docs/roles/developer/examples/nearlib/introduction

## Join the Network

The easiest way to join the network, is by using `nearup` command, which you can install:

```bash
curl --proto '=https' --tlsv1.2 -sSfL https://up.near.dev | python3
```

You can join all the active networks:
* TestNet: `nearup testnet`
* BetaNet: `nearup betanet`
* DevNet: `nearup devnet`

Check `nearup` repository for [more details](https://github.com/near/nearup) how to run with or without docker.

To learn how to become validator, checkout [documentation](https://docs.nearprotocol.com/docs/validator/staking-overview).

## Development Status

This project is currently under heavy development toward MainNet launch.

We are using [ZenHub](https://zenhub.com) to manage the development process. You can either login on their website or [install ZenHub extension](https://www.zenhub.com/extension) to see additional information right on Github.

For the high-level roadmap, checkout [Chain&Middleware's roadmap](https://app.zenhub.com/workspaces/chainmiddleware-5cea2bcf78297c385cf0ec81/roadmap). 

## Contributing

The workflow and details of setup to contribute are described in [CONTRIBUTING.md](CONTRIBUTING.md), and security policy is described in [SECURITY.md](SECURITY.md).
To propose new protocol change or standard use [Specification & Standards repository](https://github.com/nearprotocol/NEPs). 
