"""
`jsonable` is just a cute shortening of JSON-serializable,
but it also means that the object is pure data, and does not
drag any (publicly-accessible) behavior with it.
Anything that can be serialized by the default simplejson

http://simplejson.readthedocs.org/en/latest/#encoders-and-decoders

serializer would count. But the slightly more flexible serializer
given in `jsonable_handler` allows datetimes as well as objects that
implement a `to_json()` method.

Functions which can consume a single argument that can be turned into JSON
are `jsonable consumers`. JSONable consumers will
declare the expected format of the jsonable they can consume
using JSON Schema.

JSONable boundaries are functions or methods signatures which serve as the
interface between different sub-systems in an application. If we seek to have
two components loosely-coupled, then a good guarantee of this is if
they communicate only with values, and not with complex objects. In other words,
if we can take the argument to a function, serialize and then immediately
deserialize it before handing it back to to the function, then it is said
to have a JSONable signature if it still works.

One important practical advantage of this is that it enables large
systems to be more easily decomposed into components that communicate with JSON
messages through a queue.
"""

import functools
import itertools
import collections

import jsonschema
import simplejson as json

VALIDATE = True

def jsonable_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    if hasattr(obj, 'to_json'):
        return obj.to_json()
    if isinstance(obj, collections.Iterable):
        return [x for x in obj]
    else:
        raise TypeError(
            'Object of type {0} with value of {1} is not JSON serializable'
            .format(type(obj), repr(obj))
        )

def schema(json_schema):
    def decorator(f):
        f.schema = json_schema
        return f
    return decorator

def serial_deserial(arg):
    """
    Function to serialize and immediately deserialize with
    `jsonable_handler`.
    """
    json_string = json.dumps(arg, default=jsonable_handler)
    return json.loads(json_string)

def validate(do_validation):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(arg):
            if do_validation:
                deserialized = serial_deserial(arg)
                jsonschema.validate(deserialized, f.schema)
            return f(arg)
        return wrapper
    return decorator

## EXAMPLE VALID CONSUMERS

@validate(VALIDATE)
@schema({"type" : "number"})
def number_consumer(jsonable):
    return jsonable * 2

@validate(VALIDATE)
@schema({"type" : "number"})
def number_consumer_no_return(jsonable):
    # do side effects
    pass

@validate(VALIDATE)
@schema({
    "type": "array",
    "items": { "type": "number" },
})
def seq_consumer(jsonable):
    return [x*2 for x in jsonable]

## EXAMPLE INVALID CONSUMERS

@validate(VALIDATE)
@schema({"type" : "number"})
def number_consumer_bad_return(jsonable):
    # Return a non-JSON-serializable, in this case the int function.
    return int

## TESTS

from nose.tools import raises

def assert_ijsonable(f, input, expected_output):
    """Assert that the JSONable interface is respected: serialization
    of input is effectively idempotent with respect to expected output.
    Additionally, the output must be a JSONable object.
    """
    assert f(input) == expected_output
    assert f(serial_deserial(input)) == expected_output
    if expected_output:
        json.dumps(expected_output)

def test_jsonable_handler_nested_iterables():
    nested_iterables = itertools.imap(lambda x: xrange(x+1), xrange(3))
    json_string = json.dumps(nested_iterables, default=jsonable_handler)
    deserialized = json.loads(json_string)
    assert deserialized == [[0], [0, 1], [0, 1, 2]]

def test_valid_number():
    assert_ijsonable(number_consumer, 2, 4)

@raises(jsonschema.ValidationError)
def test_invalid_string():
    number_consumer('foo')

@raises(AssertionError)
def test_non_idempotent_serialization():

    class fake_number(int):
        def __mul__(self, x):
            return 2

    obj = fake_number(2)
    # This lack of idempotent serialization/deserialization should violate the
    # JSONable interface, and therefore raise an AssertionError.
    # This is how you guarantee your objects that use custom serialization
    # still satisfy the JSONable requirement with respect to the consumer.
    assert_ijsonable(number_consumer, obj, 4)

def test_no_return():
    assert_ijsonable(number_consumer_no_return, 2, None)

@raises(TypeError)
def test_bad_return():
    assert_ijsonable(number_consumer_bad_return, 2, int)

def test_valid_array():
    assert_ijsonable(seq_consumer, [1, 2], [2, 4])

@raises(jsonschema.ValidationError)
def test_invalid_array():
    seq_consumer(['foo'])

def test_valid_iterable():
    assert_ijsonable(seq_consumer, xrange(1,3), [2, 4])

@raises(jsonschema.ValidationError)
def test_invalid_iterable():
    seq_consumer(itertools.imap(lambda x: 'foo', xrange(1,3)))

