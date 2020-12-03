// SPDX-License-Identifier: Unlicense
pragma solidity ^0.7.0;

//import "hardhat/console.sol";

contract StandardPrecompiles {
  constructor() payable {
    //console.log("Deploying StandardPrecompiles");
  }

  function test_all() public view returns(bool) {
    require(test_ecrecover(), "erroneous ecrecover precompile");
    require(test_sha256(), "erroneous sha256 precompile");
    require(test_ripemd160(), "erroneous ripemd160 precompile");
    require(test_identity(), "erroneous identity precompile");
    require(test_modexp(), "erroneous modexp precompile");
    require(test_ecadd(), "erroneous ecadd precompile");
    require(test_ecmul(), "erroneous ecmul precompile");
    require(test_ecpair(), "erroneous ecpair precompile");
    require(test_blake2f(), "erroneous blake2f precompile");
    return true;
  }

  // See: https://docs.soliditylang.org/en/develop/units-and-global-variables.html#mathematical-and-cryptographic-functions
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000001
  function test_ecrecover() public pure returns(bool) {
    bytes32 hash = hex"1111111111111111111111111111111111111111111111111111111111111111";
    bytes memory sig = hex"b9f0bb08640d3c1c00761cdd0121209268f6fd3816bc98b9e6f3cc77bf82b69812ac7a61788a0fdc0e19180f14c945a8e1088a27d92a74dce81c0981fb6447441b";
    address signer = 0x1563915e194D8CfBA1943570603F7606A3115508;
    return ecverify(hash, sig, signer);
  }

  // See: https://docs.soliditylang.org/en/develop/units-and-global-variables.html#mathematical-and-cryptographic-functions
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000002
  function test_sha256() public pure returns(bool) {
    return sha256("") == hex"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  }

  // See: https://docs.soliditylang.org/en/develop/units-and-global-variables.html#mathematical-and-cryptographic-functions
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000003
  function test_ripemd160() public pure returns(bool) {
    return ripemd160("") == hex"9c1185a5c5e9fc54612808977ee8f548b2258d31";
  }

  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000004
  function test_identity() public view returns(bool) {
    bytes memory data = hex"1111111111111111111111111111111111111111111111111111111111111111";
    return keccak256(datacopy(data)) == keccak256(data);
  }

  // See: https://eips.ethereum.org/EIPS/eip-198
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000005
  function test_modexp() public view returns(bool) {
    uint256 base = 3;
    uint256 exponent = 0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2e;
    uint256 modulus = 0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f;
    return modexp(base, exponent, modulus) == 1;
  }

  // See: https://eips.ethereum.org/EIPS/eip-196
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000006
  function test_ecadd() public view returns(bool) {
    // alt_bn128_add_chfast1:
    bytes32[2] memory result;
    result = ecadd(
      0x18b18acfb4c2c30276db5411368e7185b311dd124691610c5d3b74034e093dc9,
      0x063c909c4720840cb5134cb9f59fa749755796819658d32efc0d288198f37266,
      0x07c2b7f58a84bd6145f00c9c2bc0bb1a187f20ff2c92963a88019e7c6a014eed,
      0x06614e20c147e940f2d70da3f74c9a17df361706a4485c742bd6788478fa17d7
    );
    return result[0] == 0x2243525c5efd4b9c3d3c45ac0ca3fe4dd85e830a4ce6b65fa1eeaee202839703 && result[1] == 0x301d1d33be6da8e509df21cc35964723180eed7532537db9ae5e7d48f195c915;
  }

  // See: https://eips.ethereum.org/EIPS/eip-196
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000007
  function test_ecmul() public view returns(bool) {
    // alt_bn128_mul_chfast1:
    bytes32[2] memory result;
    result = ecmul(
      0x2bd3e6d0f3b142924f5ca7b49ce5b9d54c4703d7ae5648e61d02268b1a0a9fb7,
      0x21611ce0a6af85915e2f1d70300909ce2e49dfad4a4619c8390cae66cefdb204,
      0x00000000000000000000000000000000000000000000000011138ce750fa15c2
    );
    return result[0] == 0x070a8d6a982153cae4be29d434e8faef8a47b274a053f5a4ee2a6c9c13c31e5c && result[1] == 0x031b8ce914eba3a9ffb989f9cdd5b0f01943074bf4f0f315690ec3cec6981afc;
  }

  // See: https://eips.ethereum.org/EIPS/eip-197
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000008
  function test_ecpair() public view returns(bool) {
    // alt_bn128_pairing_jeff1:
    bytes32 result = ecpair(hex"1c76476f4def4bb94541d57ebba1193381ffa7aa76ada664dd31c16024c43f593034dd2920f673e204fee2811c678745fc819b55d3e9d294e45c9b03a76aef41209dd15ebff5d46c4bd888e51a93cf99a7329636c63514396b4a452003a35bf704bf11ca01483bfa8b34b43561848d28905960114c8ac04049af4b6315a416782bb8324af6cfc93537a2ad1a445cfd0ca2a71acd7ac41fadbf933c2a51be344d120a2a4cf30c1bf9845f20c6fe39e07ea2cce61f0c9bb048165fe5e4de877550111e129f1cf1097710d41c4ac70fcdfa5ba2023c6ff1cbeac322de49d1b6df7c2032c61a830e3c17286de9462bf242fca2883585b93870a73853face6a6bf411198e9393920d483a7260bfb731fb5d25f1aa493335a9e71297e485b7aef312c21800deef121f1e76426a00665e5c4479674322d4f75edadd46debd5cd992f6ed090689d0585ff075ec9e99ad690c3395bc4b313370b38ef355acdadcd122975b12c85ea5db8c6deb4aab71808dcb408fe3d1e7690c43d37b4ce6cc0166fa7daa");
    return result == 0x0000000000000000000000000000000000000000000000000000000000000001;
  }

  // See: https://eips.ethereum.org/EIPS/eip-152
  // See: https://etherscan.io/address/0x0000000000000000000000000000000000000009
  function test_blake2f() public view returns(bool) {
    uint32 rounds = 12;
    bytes32[2] memory h;
    h[0] = hex"48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5";
    h[1] = hex"d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b";
    bytes32[4] memory m;
    m[0] = hex"6162630000000000000000000000000000000000000000000000000000000000";
    m[1] = hex"0000000000000000000000000000000000000000000000000000000000000000";
    m[2] = hex"0000000000000000000000000000000000000000000000000000000000000000";
    m[3] = hex"0000000000000000000000000000000000000000000000000000000000000000";
    bytes8[2] memory t;
    t[0] = hex"03000000";
    t[1] = hex"00000000";
    bool f = true;
    bytes32[2] memory result = blake2f(rounds, h, m, t, f);
    return result[0] == hex"ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d1" && result[1] == hex"7d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923";
  }

  function ecverify(bytes32 hash, bytes memory sig, address signer) private pure returns (bool) {
    bool ok;
    address addr;
    (ok, addr) = ecrecovery(hash, sig);
    return ok && addr == signer;
  }

  function ecrecovery(bytes32 hash, bytes memory sig) private pure returns (bool, address) {
    if (sig.length != 65)
      return (false, address(0));

    bytes32 r;
    bytes32 s;
    uint8 v;
    assembly {
      r := mload(add(sig, 32))
      s := mload(add(sig, 64))
      v := byte(0, mload(add(sig, 96)))
    }

    address addr = ecrecover(hash, v, r, s);
    return (true, addr);
  }

  function datacopy(bytes memory input) private view returns (bytes memory) {
    bytes memory output = new bytes(input.length);
    assembly {
      let len := mload(input)
      let ok := staticcall(gas(), 0x04, add(input, 32), len, add(output, 32), len)
      switch ok
      case 0 {
        revert(0, 0)
      }
    }
    return output;
  }

  function modexp(uint256 base, uint256 exponent, uint256 modulus) private view returns (uint256 output) {
    assembly {
      let ptr := mload(0x40)
      mstore(ptr, 32)
      mstore(add(ptr, 0x20), 32)
      mstore(add(ptr, 0x40), 32)
      mstore(add(ptr, 0x60), base)
      mstore(add(ptr, 0x80), exponent)
      mstore(add(ptr, 0xA0), modulus)
      let ok := staticcall(gas(), 0x05, ptr, 0xC0, ptr, 0x20)
      switch ok
      case 0 {
        revert(0, 0)
      }
      default {
        output := mload(ptr)
      }
    }
  }

  function ecadd(bytes32 ax, bytes32 ay, bytes32 bx, bytes32 by) private view returns (bytes32[2] memory output) {
    bytes32[4] memory input = [ax, ay, bx, by];
    assembly {
      let ok := staticcall(gas(), 0x06, input, 0x80, output, 0x40)
      switch ok
      case 0 {
        revert(0, 0)
      }
    }
  }

  function ecmul(bytes32 x, bytes32 y, bytes32 scalar) private view returns (bytes32[2] memory output) {
    bytes32[3] memory input = [x, y, scalar];
    assembly {
      let ok := staticcall(gas(), 0x07, input, 0x60, output, 0x40)
      switch ok
      case 0 {
        revert(0, 0)
      }
    }
  }

  function ecpair(bytes memory input) private view returns (bytes32 output) {
    uint256 len = input.length;
    require(len % 192 == 0);
    assembly {
      let ptr := mload(0x40)
      let ok := staticcall(gas(), 0x08, add(input, 32), len, ptr, 0x20)
      switch ok
      case 0 {
        revert(0, 0)
      }
      default {
        output := mload(ptr)
      }
    }
  }

  function blake2f(uint32 rounds, bytes32[2] memory h, bytes32[4] memory m, bytes8[2] memory t, bool f) private view returns (bytes32[2] memory) {
    bytes32[2] memory output;
    bytes memory args = abi.encodePacked(rounds, h[0], h[1], m[0], m[1], m[2], m[3], t[0], t[1], f);
    assembly {
      let ok := staticcall(gas(), 0x09, add(args, 32), 0xD5, output, 0x40)
      switch ok
      case 0 {
        revert(0, 0)
      }
    }
    return output;
  }
}
