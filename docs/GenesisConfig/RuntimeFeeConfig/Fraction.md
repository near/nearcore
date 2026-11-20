# Fraction

A rational number. In code it is a `Rational32`.

In the genesis config and in RPC responses it is a JSON array of two signed 32-bit integers, `[numerator, denominator]`.
For example, `[3, 10]` means 3/10.

The parameter files in `core/parameters/res/runtime_configs/` write the same value as an object with `numerator` and `denominator` keys.
