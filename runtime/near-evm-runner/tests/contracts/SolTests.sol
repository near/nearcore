pragma solidity ^0.5.8;

contract ExposesBalance {
  function balance() public view returns (uint256) {
      return address(this).balance;
  }

  // Unpermissioned. Don't deploy for real :D
  function transferTo(address _recipient, uint256 _amount) public returns (uint256) {
      address(uint160(_recipient)).transfer(_amount);
      return balance();
  }
}

contract SolTests is ExposesBalance {
    constructor() public payable {}

    event SomeEvent(uint256 _number);

    event DeployEvent(
        address indexed _contract,
        uint256 _amount
    );

    function () external payable {}

    function deployNewGuy(uint256 _aNumber) public payable returns (address, uint256) {
        SubContract _newGuy = new SubContract(_aNumber);
        address(_newGuy).transfer(msg.value);
        emit DeployEvent(address(_newGuy), msg.value);
        return (address(_newGuy), msg.value);
    }

    function payNewGuy(uint256 _aNumber) public payable returns (address, uint256) {
        SubContract _newGuy = (new SubContract).value(msg.value)(_aNumber);
        return (address(_newGuy), msg.value);
    }

    function returnSomeFunds() public payable returns (address, uint256) {
        address(msg.sender).transfer(msg.value / 2);
        return (msg.sender, msg.value);
    }

    function emitIt(uint256 _aNumber) public returns (bool) {
        emit SomeEvent(_aNumber);
        return true;
    }

    function precompileTest() public pure {
        require(sha256("") == hex"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "sha2 digest mismatch");
        require(ripemd160("") == hex"9c1185a5c5e9fc54612808977ee8f548b2258d31", "rmd160 digest mismatch");

        // signed with hex"2222222222222222222222222222222222222222222222222222222222222222"
        address addr = ecrecover(
            hex"1111111111111111111111111111111111111111111111111111111111111111",
            27,
            hex"b9f0bb08640d3c1c00761cdd0121209268f6fd3816bc98b9e6f3cc77bf82b698", // r
            hex"12ac7a61788a0fdc0e19180f14c945a8e1088a27d92a74dce81c0981fb644744"  // s
        );

        require(
            addr == 0x1563915e194D8CfBA1943570603F7606A3115508,
            "ecrecover mismatch"
        );
    }
}

contract SubContract is ExposesBalance {
    uint256 public aNumber = 6;

    constructor(uint256 _aNumber) public payable {
        aNumber = _aNumber;
    }

    function aFunction() public pure returns (bool) {
        return true;
    }

    function () external payable {}
}

contract PrecompiledFunction {
    constructor() public payable {}

    function noop() public pure {
        
    }

    function testSha256() public pure returns (bytes32) {
        return sha256("");
    }

    function testSha256_100bytes() public pure returns (bytes32) {
        return sha256("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvw");
    }

    function testEcrecover() public pure returns (address) {
        return ecrecover(
            hex"1111111111111111111111111111111111111111111111111111111111111111",
            27,
            hex"b9f0bb08640d3c1c00761cdd0121209268f6fd3816bc98b9e6f3cc77bf82b698", // r
            hex"12ac7a61788a0fdc0e19180f14c945a8e1088a27d92a74dce81c0981fb644744"  // s
        );
    }

    function testRipemd160() public pure returns (bytes20) {
        return ripemd160("");
    }

    function testRipemd160_1kb() public pure returns (bytes20) {
        return ripemd160("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghij");
    }

    function identity(bytes memory data) public returns (bytes memory) {
        bytes memory ret = new bytes(data.length);
        assembly {
            let len := mload(data)
            if iszero(call(gas, 0x04, 0, add(data, 0x20), len, add(ret,0x20), len)) {
                invalid()
            }
        }

        return ret;
    }

    function test_identity() public returns (bytes memory) {
        return identity("");
    }

    function test_identity_100bytes() public returns (bytes memory) {
        return identity("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvw");
    }

    function modexp(uint base, uint e, uint m) public view returns (uint o) {
        assembly {
            // define pointer
            let p := mload(0x40)
            // store data assembly-favouring ways
            mstore(p, 0x20)             // Length of Base
            mstore(add(p, 0x20), 0x20)  // Length of Exponent
            mstore(add(p, 0x40), 0x20)  // Length of Modulus
            mstore(add(p, 0x60), base)  // Base
            mstore(add(p, 0x80), e)     // Exponent
            mstore(add(p, 0xa0), m)     // Modulus
            if iszero(staticcall(sub(gas, 2000), 0x05, p, 0xc0, p, 0x20)) {
                revert(0, 0)
            }
            // data
            o := mload(p)
        }
    }

    function testModExp() public view returns (uint) {
        return modexp(12345, 173, 101);
    }

    function testBn128Add() public pure returns (uint) {

    }
}