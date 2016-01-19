#!/usr/bin/env python
"""
util functions and helpers

"""
import uuid
import inspect


short_uuid = lambda: str(uuid.uuid4()).rsplit('-', 1)[1]


def object_name(ref):
    """
    _object_name_

    Get the module.Class or module.func name for
    the object passed in
    """
    if inspect.isfunction(ref):
        ref_name = ref.__name__
        ref_mod = ref.__module__
        return "{mod}.{cls}".format(mod=ref_mod, cls=ref_name)
    if inspect.isclass(ref):
        # class could use inspect.isclass(op) here I guess
        ref_name = ref.__name__
        ref_mod = ref.__module__
    else:
        # instance
        ref_name = type(ref).__name__
        ref_mod = type(ref).__module__
    return "{mod}.{cls}".format(mod=ref_mod, cls=ref_name)
