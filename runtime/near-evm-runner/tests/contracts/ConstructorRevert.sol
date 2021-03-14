pragma solidity ^0.5.8;

contract ConstructorRevert {
    constructor() public {
        require(2 + 2 == 5, "Error Deploying Contract");
    }
}
