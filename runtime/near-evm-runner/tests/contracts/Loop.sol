pragma solidity ^0.7.0;
contract Loop {
    function run() public payable returns(uint) {
        uint counter = 0;
        while (true) {
            counter += 1;
        }
        return counter;
    }
}
