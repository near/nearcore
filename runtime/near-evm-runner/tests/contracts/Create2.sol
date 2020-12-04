pragma solidity ^0.5.8;

contract Create2Factory {
    function deploy(bytes32 _salt, bytes memory _contractBytecode) public returns (address payable addr) {
        assembly {
            addr := create2(0, add(_contractBytecode, 0x20), mload(_contractBytecode), _salt)
        }
    }

    function testDoubleDeploy(bytes32 _salt, bytes memory _contractBytecode) public payable returns (bool) {
        // deploy
        address payable addr = deploy(_salt, _contractBytecode);
        SelfDestruct other = SelfDestruct(addr);

        // use once
        other.storeUint(5);
        require(other.storedUint() == 5, "pre-destruction wrong uint");

        // destroy and check if still exists
        other.destruction();
        bytes memory code = getCode(address(other));
        require(code.length == 0, "post-destruction code length");

        // redeploy and use again
        deploy(_salt, _contractBytecode);
        require(other.storedUint() == 0, "post-destruction wrong uint");
        other.storeUint(3);
        require(other.storedUint() == 3, "post-destruction stored wrong uint");
        return true;
    }

    function getCode(address other) internal view returns (bytes memory code) {
        assembly {
            code := mload(0x40)
            let size := extcodesize(other)
            mstore(code, size)
            let body := add(code, 0x20)
            extcodecopy(other, body, 0, size)
            mstore(code, size)
            mstore(0x40, add(body, size))
        }
    }


    function () external payable {}

}

contract SelfDestruct {
    address public storedAddress;
    uint public storedUint;

    function () external payable {}

    function storeAddress() public {
        storedAddress = msg.sender;
    }

    function storeUint(uint _number) public {
        storedUint = _number;
    }

    function destruction() public {
        selfdestruct(msg.sender);
    }
}
