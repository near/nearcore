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

    function () external payable {}

    function deployNewGuy(uint256 _aNumber) public payable returns (address, uint256) {
        SubContract _newGuy = new SubContract(_aNumber);
        address(_newGuy).transfer(msg.value);
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
