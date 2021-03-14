// Source: https://raw.githubusercontent.com/web3j/web3j/master/codegen/src/test/resources/solidity/fibonacci/Fibonacci.sol

pragma solidity ^0.4.2;

contract Fibonacci {

    event Notify(uint input, uint result);

    function fibonacci(uint number) public constant returns(uint result) {
        if (number == 0) return 0;
        else if (number == 1) return 1;
        else return Fibonacci.fibonacci(number - 1) + Fibonacci.fibonacci(number - 2);
    }

    function fibonacciNotify(uint number) public returns(uint result) {
        result = fibonacci(number);
        emit Notify(number, result);
    }
}
