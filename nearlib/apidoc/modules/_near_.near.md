[nearlib](../README.md) > ["near"](../modules/_near_.md) > [near](../modules/_near_.near.md)

# Module: near

## Index

### Functions

* [base58](_near_.near.md#base58)
* [bufferWithSize](_near_.near.md#bufferwithsize)
* [bufferWithSizeFromPtr](_near_.near.md#bufferwithsizefromptr)
* [hash](_near_.near.md#hash)
* [hash32](_near_.near.md#hash32)
* [log](_near_.near.md#log)
* [random32](_near_.near.md#random32)
* [randomBuffer](_near_.near.md#randombuffer)
* [str](_near_.near.md#str)
* [utf8](_near_.near.md#utf8)

---

## Functions

<a id="base58"></a>

###  base58

▸ **base58**(source: *`Uint8Array`*): `string`

*Defined in near.ts:142*

**Parameters:**

| Name | Type |
| ------ | ------ |
| source | `Uint8Array` |

**Returns:** `string`

___
<a id="bufferwithsize"></a>

###  bufferWithSize

▸ **bufferWithSize**(buf: *`Uint8Array`*): `Uint8Array`

*Defined in near.ts:91*

**Parameters:**

| Name | Type |
| ------ | ------ |
| buf | `Uint8Array` |

**Returns:** `Uint8Array`

___
<a id="bufferwithsizefromptr"></a>

###  bufferWithSizeFromPtr

▸ **bufferWithSizeFromPtr**(ptr: *`usize`*, length: *`usize`*): `Uint8Array`

*Defined in near.ts:81*

**Parameters:**

| Name | Type |
| ------ | ------ |
| ptr | `usize` |
| length | `usize` |

**Returns:** `Uint8Array`

___
<a id="hash"></a>

###  hash

▸ **hash**<`T`>(data: *`T`*): `Uint8Array`

*Defined in near.ts:108*

**Type parameters:**

#### T 
**Parameters:**

| Name | Type |
| ------ | ------ |
| data | `T` |

**Returns:** `Uint8Array`

___
<a id="hash32"></a>

###  hash32

▸ **hash32**<`T`>(data: *`T`*): `u32`

*Defined in near.ts:121*

**Type parameters:**

#### T 
**Parameters:**

| Name | Type |
| ------ | ------ |
| data | `T` |

**Returns:** `u32`

___
<a id="log"></a>

###  log

▸ **log**(msg: *`string`*): `void`

*Defined in near.ts:95*

**Parameters:**

| Name | Type |
| ------ | ------ |
| msg | `string` |

**Returns:** `void`

___
<a id="random32"></a>

###  random32

▸ **random32**(): `u32`

*Defined in near.ts:138*

**Returns:** `u32`

___
<a id="randombuffer"></a>

###  randomBuffer

▸ **randomBuffer**(len: *`u32`*): `Uint8Array`

*Defined in near.ts:132*

**Parameters:**

| Name | Type |
| ------ | ------ |
| len | `u32` |

**Returns:** `Uint8Array`

___
<a id="str"></a>

###  str

▸ **str**<`T`>(value: *`T`*): `string`

*Defined in near.ts:99*

**Type parameters:**

#### T 
**Parameters:**

| Name | Type |
| ------ | ------ |
| value | `T` |

**Returns:** `string`

___
<a id="utf8"></a>

###  utf8

▸ **utf8**(value: *`string`*): `usize`

*Defined in near.ts:104*

**Parameters:**

| Name | Type |
| ------ | ------ |
| value | `string` |

**Returns:** `usize`

___

