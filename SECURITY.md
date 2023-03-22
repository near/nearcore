# Security Policy

Reference client for NEAR is held to the highest security standard.
This document defines the policy on how to report vulnerabilities and receive updates when security patches are released.

If you have any suggestions or comments about the security policy, please email the [NEAR Security Team](mailto:security@near.org) at security@near.org

## Reporting a vulnerability

All security issues should be submitted on [hackenproof](https://hackenproof.com/near/near-protocol). The team will review the submissions and decide whether they are eligible for bounty payouts. For more details, please check out the program description on the hackenproof website.

## Handling & disclosure process

1. Security report is received and assigned to an owner. This person will coordinate the process of evaluating, fixing, releasing and disclosing the issue.
2. After initial report is received, the evaluation process is performed. It's identified if the issue exists, its severity and which version / components of the code is affected. Additional review to identify similar issues also happens. 
3. Fixes are implemented for all supported releases. These fixes are not publicly communicated but held in a private repo of the Security Team or locally.
4. A suggested announcement date for this vulnerability is chosen. The notification is drafted and includes patches to all supported versions and affected components.
5. On the announcement date, the [NEAR Security Update newsletter](https://groups.google.com/a/near.org/g/security-updates) is sent an announcement. The changes are fast tracked and merged into the public repository. At least 6 hours after the mailing list is notified, a copy of the advisory will be published across social channels. 

This process may take time, especially when coordinating with network participants and maintainers of other components in the ecosystem.
The goal will be to address issues in as short of a period as possible, but it's important that the process described above to ensure that disclosures are handled in a consistent manner.  

*Note:* If the Security Team identifies that an issue is mission-critical and requires a subset of network participants to update prior to newsletter announcement - this will be done in a manual way by communicating via direct channels. 

## Reward

The discovery of the security vulnerabilities that include but are not limited to the following categories will be rewarded proportionally to their severity:
* Algorithmic, implementation, and economic issues that violate safety of the blockchain;
* Algorithmic, implementation, and economic issues that stall the blockchain or significantly throttle liveness;
* Algorithmic, implementation, and economic issues in the standard contracts developed by NEAR;
* Issues that expose the private data of the users, the developers, or the validators;

The following are the necessary conditions for the reward:
* The vulnerability is disclosed to NEAR before it is disclosed publicly and NEAR is given sufficient time to fix it;
* The vulnerability is not disclosed to anyone else except the finder and NEAR before it is fixed;
* The vulnerability is not exploited until it is fixed.

### Rewards platform

We are using https://gitcoin.co/ to reward tokens. Meaning that every security vulnerability that you submit to us will be processed like a general work-item by an external contributor. To receive the reward, you would need to register on https://gitcoin.co/ and be able to receive the reward through it. Example of a reward for a security vulnerability finding: https://gitcoin.co/issue/near/community/5/4359


## Receive Security Updates

If you want to be informed about security vulnerabilities, please subscribe to the [NEAR Security Update newsletter](https://groups.google.com/a/near.org/g/security-updates).
The newsletter is very low traffic and only sent out where public disclosure of a vulnerability happens.
