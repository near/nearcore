# When to use private Github repository

<blockquote style="background: rgba(255, 200, 0, 0.1); border: 5px solid rgba(255, 200, 0, 0.4);">

The intended audience of the information presented here is developers working
on the implementation of the NEAR protocol.

Are you a security researcher? Please report security vulnerabilities to
[security@near.org](mailto:security@near.org).

</blockquote>

## TL;DR; 

Limit usage of nearcore-private to sensitive issues that need to be kept confidential


## Details

By nature, NEAR protocol ecosystem is community driven effort, welcoming everyone to make contribution; we believe in power of community and openness and bring a greater good. 
In this regard, by default, contributors create an issue/PR in a public space, such as [nearcore](https://github.com/near/nearcore).

However, from time to time, if a sensitive issue is discovered (e.g. security vulnerability), it cannot be tracked publicly; we need to limit the blast radius and conceal the issue from malicious actors. 
For this purpose, we maintain a private fork, [nearcore-private](https://github.com/near/nearcore-private), of public repository, nearcore, without any changes on top, and track such issues/PRs, in essence all sensitive development. 

Due to criticality of its contents, the private repository has limited accessibility and special attention is needed on deciding when to use the private repository.

Before creating an issue under nearcore-private, ask the following questions yourself:
* Does it contain information that can be exploited by malicious actors?
* Does it need special attention beyond what's provided from public repository due to its sensitivity?
* Does confidentiality of the issue overrule our principle of public transparency?

Unless an answer to one of these questions is 'YES', the issue may better reside within public repository. If you are unsure, consult private repo auditors (e.g. @akhi3030, @mm-near) to get their thoughts.

## Extra
To learn more about the process for creating an issue/PR on nearcore-private, please visit [link](https://github.com/near/nearcore/blob/c308df157bf64a528033b618b4f444d3b9c73f94/docs/practices/security_vulnerabilities.md).
