# Small library for borsh serialization and deserialization.

import logging
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import new_logger

logger = new_logger('serializer', level=logging.INFO)


class BinarySerializer:

    def __init__(self, schema):
        self.array = bytearray()
        self.schema = schema

    def read_bytes(self, n):
        assert n + self.offset <= len(
            self.array
        ), f'n: {n} offset: {self.offset}, length: {len(self.array)}'
        ret = self.array[self.offset:self.offset + n]
        self.offset += n
        return ret

    def serialize_num(self, value, n_bytes):
        assert value >= 0
        for i in range(n_bytes):
            self.array.append(value & 255)
            value //= 256
        assert value == 0

    def deserialize_num(self, n_bytes):
        logger.debug(f"deserialize_num {n_bytes}")

        value = 0
        bytes_ = self.read_bytes(n_bytes)
        for b in bytes_[::-1]:
            value = value * 256 + b
        return value

    def serialize_field(self, value, fieldType):
        if type(fieldType) == tuple:
            if len(fieldType) == 0:
                pass
            else:
                assert len(value) == len(fieldType)
                for (v, t) in zip(value, fieldType):
                    self.serialize_field(v, t)
        elif type(fieldType) == str:
            if fieldType == 'bool':
                assert isinstance(value, bool), str(type(value))
                self.serialize_num(int(value), 1)
            elif fieldType[0] == 'u':
                self.serialize_num(value, int(fieldType[1:]) // 8)
            elif fieldType == 'string':
                b = value.encode('utf8')
                self.serialize_num(len(b), 4)
                self.array += b
            else:
                assert False, fieldType
        elif type(fieldType) == list:
            assert len(fieldType) == 1
            if type(fieldType[0]) == int:
                assert type(value) == bytes
                assert len(value) == fieldType[0], "len(%s) = %s != %s" % (
                    value, len(value), fieldType[0])
                self.array += bytearray(value)
            else:
                self.serialize_num(len(value), 4)
                for el in value:
                    self.serialize_field(el, fieldType[0])
        elif type(fieldType) == dict:
            assert fieldType['kind'] == 'option'
            if value is None:
                self.serialize_num(0, 1)
            else:
                self.serialize_num(1, 1)
                self.serialize_field(value, fieldType['type'])
        elif type(fieldType) == type:
            assert type(value) == fieldType, "%s != type(%s)" % (fieldType,
                                                                 value)
            self.serialize_struct(value)
        else:
            assert False, type(fieldType)

    def deserialize_field(self, fieldType):
        logger.debug(f"deserialize_field {fieldType} {type(fieldType)}")

        if type(fieldType) == tuple:
            if len(fieldType) == 0:
                return None
            else:
                return tuple(self.deserialize_field(t) for t in fieldType)

        elif type(fieldType) == str:
            if fieldType == 'bool':
                value = self.deserialize_num(1)
                assert 0 <= value <= 1, f"Fail to deserialize bool: {value}"
                return bool(value)
            elif fieldType[0] == 'u':
                return self.deserialize_num(int(fieldType[1:]) // 8)
            elif fieldType == 'string':
                len_ = self.deserialize_num(4)
                return self.read_bytes(len_).decode('utf8')
            else:
                assert False, fieldType
        elif type(fieldType) == list:
            assert len(fieldType) == 1
            if type(fieldType[0]) == int:
                return bytes(self.read_bytes(fieldType[0]))
            else:
                len_ = self.deserialize_num(4)
                return [
                    self.deserialize_field(fieldType[0]) for _ in range(len_)
                ]
        elif type(fieldType) == dict:
            assert fieldType['kind'] == 'option'
            is_none = self.deserialize_num(1) == 0
            if is_none:
                return None
            else:
                return self.deserialize_field(fieldType['type'])
        elif type(fieldType) == type:
            return self.deserialize_struct(fieldType)
        else:
            assert False, type(fieldType)

    def serialize_struct(self, obj):
        structSchema = self.schema[type(obj)]
        if structSchema['kind'] == 'struct':
            for fieldName, fieldType in structSchema['fields']:
                try:
                    self.serialize_field(getattr(obj, fieldName), fieldType)
                except AssertionError as exc:
                    raise AssertionError(f"Error in field {fieldName}") from exc
        elif structSchema['kind'] == 'enum':
            name = getattr(obj, structSchema['field'])
            for idx, (fieldName,
                      fieldType) in enumerate(structSchema['values']):
                if fieldName == name:
                    self.serialize_num(idx, 1)
                    try:
                        self.serialize_field(getattr(obj, fieldName), fieldType)
                    except AssertionError as exc:
                        raise AssertionError(
                            f"Error in field {fieldName}") from exc
                    break
            else:
                assert False, name
        else:
            assert False, structSchema

    def deserialize_struct(self, type_):
        structSchema = self.schema[type_]
        logger.debug(f"deserialize_struct {type_} {structSchema['kind']}")

        if structSchema['kind'] == 'struct':
            ret = type_()
            for fieldName, fieldType in structSchema['fields']:
                setattr(ret, fieldName, self.deserialize_field(fieldType))
            return ret
        elif structSchema['kind'] == 'enum':
            value_ord = self.deserialize_num(1)
            if (value_ord < 0) or (len(structSchema['values']) <= value_ord):
                raise IndexError(
                    f"Unknown enum variant {value_ord} for {type_}, num variants: {len(structSchema['values'])}"
                )

            logger.debug(
                f"deserialize_struct {type_} {structSchema['kind']} enum value ord {value_ord} struct schema {len(structSchema['values'])}"
            )
            value_schema = structSchema['values'][value_ord]
            logger.debug(
                f"deserialize_struct {type_} {structSchema['kind']} enum value sch {value_schema}"
            )

            if value_schema is None:
                raise IndexError(f"value schema missing for type {type_}")

            if value_schema[1] is None:
                raise IndexError(
                    f"value schema not supported for {type_}::{value_schema[0]}"
                )

            ret = type_()
            setattr(ret, structSchema['field'], value_schema[0])
            setattr(ret, value_schema[0],
                    self.deserialize_field(value_schema[1]))

            return ret
        else:
            assert False, structSchema

    def serialize(self, obj):
        self.serialize_struct(obj)
        return bytes(self.array)

    def deserialize(self, bytes_, type_):
        self.array = bytearray(bytes_)
        self.offset = 0
        ret = self.deserialize_field(type_)
        assert self.offset == len(bytes_), "%s != %s" % (self.offset,
                                                         len(bytes_))
        return ret
