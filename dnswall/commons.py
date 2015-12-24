#!/usr/bin/env python

import __builtin__ as builtins
import itertools
import re
import socket
import sys
from contextlib import closing
from datetime import datetime
from functools import reduce

__author__ = 'Julien Palard <julien@eeple.fr>'
__all__ = [
    'Pipe', 'take', 'tail', 'skip',
    'all', 'any', 'avg', 'count', 'max', 'min', 'permutations',
    'netcat', 'netwrite',
    'traverse', 'join', 'split', 'replace',
    'as_int', 'as_str', 'as_list', 'as_tuple', 'as_dict',
    'stdout', 'lineout',
    'tee', 'add', 'first', 'chain', 'collect', 'select', 'take_while',
    'skip_while', 'aggregate', 'groupby', 'sort', 'reverse',
    'chain_with', 'islice', 'izip', 'passed', 'index', 'strip',
    'lstrip', 'rstrip', 'run_with', 't', 'to_type',
]


class Pipe:
    """
    Represent a Pipeable Element :
    Described as :
    first = Pipe(lambda iterable: next(iter(iterable)))
    and used as :
    print [1, 2, 3] | first
    printing 1

    Or represent a Pipeable Function :
    It's a function returning a Pipe
    Described as :
    collect = Pipe(lambda iterable, pred: (pred(x) for x in iterable))
    and used as :
    print [1, 2, 3] | collect(lambda x: x * 2)
    # 2, 4, 6
    """

    def __init__(self, function):
        self.function = function

    def __ror__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        return Pipe(lambda x: self.function(x, *args, **kwargs))


@Pipe
def take(iterable, qte):
    "Yield qte of elements in the given iterable."
    for item in iterable:
        if qte > 0:
            qte -= 1
            yield item
        else:
            return


@Pipe
def tail(iterable, qte):
    "Yield qte of elements in the given iterable."
    out = []
    for item in iterable:
        out.append(item)
        if len(out) > qte:
            out.pop(0)
    return out


@Pipe
def skip(iterable, qte):
    "Skip qte elements in the given iterable, then yield others."
    for item in iterable:
        if qte == 0:
            yield item
        else:
            qte -= 1


@Pipe
def all(iterable, pred):
    "Returns True if ALL elements in the given iterable are true for the given pred function"
    return builtins.all(pred(x) for x in iterable)


@Pipe
def any(iterable, pred):
    "Returns True if ANY element in the given iterable is True for the given pred function"
    return builtins.any(pred(x) for x in iterable)


@Pipe
def avg(iterable):
    """
    Build the avg for the given iterable, starting with 0.0 as seed
    Will try a division by 0 if the iterable is empty...
    """
    total = 0.0
    qte = 0
    for x in iterable:
        total += x
        qte += 1
    return total / qte


@Pipe
def count(iterable):
    "Count the size of the given iterable, walking thrue it."
    _count = 0
    for x in iterable:
        _count += 1
    return _count


@Pipe
def max(iterable, **kwargs):
    return builtins.max(iterable, **kwargs)


@Pipe
def min(iterable, **kwargs):
    return builtins.min(iterable, **kwargs)


@Pipe
def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    for x in itertools.permutations(iterable, r):
        yield x


@Pipe
def netcat(to_send, host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.connect((host, port))
        for data in to_send | traverse:
            s.send(data)
        while 1:
            data = s.recv(4096)
            if not data: break
            yield data


@Pipe
def netwrite(to_send, host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.connect((host, port))
        for data in to_send | traverse:
            s.send(data)


@Pipe
def traverse(args):
    for arg in args:
        try:
            if isinstance(arg, str):
                yield arg
            else:
                for i in arg | traverse:
                    yield i
        except TypeError:
            # not iterable --- output leaf
            yield arg


@Pipe
def join(iterable, separator):
    return separator.join(builtins.map(str, iterable))


@Pipe
def split(iterable, pattern, maxsplit=0):
    return re.split(pattern, iterable, maxsplit=maxsplit)


@Pipe
def replace(to_replace, pattern, replacement):
    return re.sub(pattern, replacement, to_replace)


@Pipe
def as_int(obj):
    return builtins.int(obj)


@Pipe
def as_str(obj):
    return builtins.str(obj)


@Pipe
def as_timestamp(obj):
    if not isinstance(obj, datetime):
        raise TypeError('obj must be a datetime.')
    else:
        delta = obj - datetime.utcfromtimestamp(0)
        return delta.seconds + delta.days * 24 * 3600


@Pipe
def as_list(iterable):
    return builtins.list(iterable)


@Pipe
def as_tuple(iterable):
    return builtins.tuple(iterable)


@Pipe
def as_dict(iterable):
    return dict(iterable)


@Pipe
def stdout(x):
    sys.stdout.write(str(x))


@Pipe
def lineout(x):
    sys.stdout.write(str(x) + "\n")


@Pipe
def tee(iterable):
    for item in iterable:
        sys.stdout.write(str(item) + "\n")
        yield item


@Pipe
def add(x):
    return builtins.sum(x)


@Pipe
def first(iterable):
    try:
        return builtins.next(iter(iterable))
    except StopIteration:
        return None


@Pipe
def chain(iterable):
    return itertools.chain(*iterable)


@Pipe
def collect(iterable, selector):
    return (selector(x) for x in iterable)


@Pipe
def select(iterable, predicate):
    return (x for x in iterable if (predicate(x)))


@Pipe
def take_while(iterable, predicate):
    return itertools.takewhile(predicate, iterable)


@Pipe
def skip_while(iterable, predicate):
    return itertools.dropwhile(predicate, iterable)


@Pipe
def aggregate(iterable, function, **kwargs):
    if 'initializer' in kwargs:
        return reduce(function, iterable, kwargs['initializer'])
    else:
        return reduce(function, iterable)


@Pipe
def groupby(iterable, keyfunc):
    return itertools.groupby(sorted(iterable, key=keyfunc), keyfunc)


@Pipe
def sort(iterable, **kwargs):
    return sorted(iterable, **kwargs)


@Pipe
def reverse(iterable):
    return reversed(iterable)


@Pipe
def passed(x):
    pass


@Pipe
def index(iterable, value, start=0, stop=None):
    return iterable.index(value, start, stop or len(iterable))


@Pipe
def strip(iterable, chars=None):
    return iterable.strip(chars)


@Pipe
def rstrip(iterable, chars=None):
    return iterable.rstrip(chars)


@Pipe
def lstrip(iterable, chars=None):
    return iterable.lstrip(chars)


@Pipe
def run_with(iterable, func):
    return func(**iterable) if isinstance(iterable, dict) else \
        func(*iterable) if hasattr(iterable, '__iter__') else \
            func(iterable)


@Pipe
def t(iterable, y):
    if hasattr(iterable, '__iter__') and not isinstance(iterable, str):
        return iterable + type(iterable)([y])
    else:
        return [iterable, y]


@Pipe
def to_type(x, t):
    return t(x)


chain_with = Pipe(itertools.chain)
islice = Pipe(itertools.islice)

# Python 2 & 3 compatibility
if "izip" in dir(itertools):
    izip = Pipe(itertools.izip)
else:
    izip = Pipe(zip)

if __name__ == "__main__":
    import doctest

    doctest.testmod()
