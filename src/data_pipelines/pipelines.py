#!/usr/bin/env python
"""
_pipelines_

Util iterators for composing functions into
data processing pipelines.
Think things like map/filter/reduce but all pumped up
to be a bit more generator friendly

"""
import itertools
from .utilities import object_name, short_uuid


class Pipeline(object):
    """
    _Pipeline_

    Container for a chain of pipeline operators,
    holds onto the first and last operator in the chain
    and exposes the same iterable, chain and execute API
    as a PipelineOperator

    """
    def __init__(self, first, last, label=None):
        self.start = first
        self.end = last
        self.label = label or short_uuid()

    def __iter__(self):
        return self.end

    def chain(self, input_iter):
        self.start.chain(input_iter)

    def execute(self):
        return self.end.execute()

    def to_json(self):
        """
        create a JSON configuration representing
        this pipeline and its content
        """
        return {
            'type': type(self).__name__,
            'label': self.label,
            'content': self.end.to_json(),
        }

    @staticmethod
    def from_configuration(cls, config):
        """
        from_configuration

        Return a Pipeline instance populated with operators
        as defined in the configuration JSON
        """
        pass


class PipelineOperator(object):
    """
    Pipeline Operator that processes or modifies
    the data elements it sees during iteration.

    Also the base class for other pipeline operator
    types.

    Defines iter and next to act as an iterator and
    executes the action function on each element
    of the input iterable before passing it onwards

    Trivial Example usage:

    def print_elem(elem):
        print elem

    input_iter =  (x for x in range(10))

    pipeline = PipelineOperator(action=print_elem)
    pipeline.chain(input_iter)

    # returns a list of [1..10] and prints each element
    results = pipeline.execute()

    """
    def __init__(self, action=lambda x: x):
        super(PipelineOperator, self).__init__()
        self.action = action
        self.input = None

    def __iter__(self):
        return self

    def next(self):
        """
        _next_

        Implements the iterator protocol
        consume the next value from the input,
        call action on it, return the value
        """
        value = self.input.next()
        self.action(value)
        return value

    def chain(self, oper):
        """
        _chain_

        Chain another iterable as input to this operator
        """
        self.input = oper

    def execute(self):
        """
        _execute_

        Run this objects iterator, which will
        exhaust all the iterators in the pipelin
        and return the output as a list
        """
        return [x for x in self]

    def to_json(self):
        """
        _to_json_

        """
        result = {
            "type": type(self).__name__,
            "action": object_name(self.action),
        }
        if isinstance(self.input, PipelineOperator):
            result['input'] = self.input.to_json()
        return result


class PipelineTransform(PipelineOperator):
    """
    Pipeline operator that replaces the data elements
    with the return result of the action defined
    within it, compared to the base class which ignores the
    return value, IE it transforms the element using action,
    rather than just calls the action for each
    """
    def __init__(self, action=lambda x: x):
        super(PipelineTransform, self).__init__(action)

    def next(self):
        value = self.input.next()
        return self.action(value)


class PipelineFilter(PipelineOperator):
    """
    filter pipeline operator that will only pass onwards
    elements for which the action function evaluates to True

    This is basically used to clean out elements that dont match
    some criteria defined in the action function

    """
    def __init__(self, action=lambda x: True):
        super(PipelineFilter, self).__init__(action)

    def next(self):
        """
        _next_

        Implement the iterator protocol, but only
        yield on if the action evaluates to True, if not,
        advance to the next element in the input and repeat
        until there is a match or the iteration ends
        """
        found_pass = False
        value = None
        while not found_pass:
            try:
                value = self.input.next()
            except StopIteration:
                # this is kind of implied, but easier to see
                # what happens with this catch/raise
                raise
            found_pass = self.action(value)
        return value


class PipelineMap(PipelineOperator):
    """
    _PipelineMap_

    Given an input iterable/pipeline,
    tee it to several sub pipelines, and make those
    pipelines iterate in step in a sane way.

    """
    def __init__(self, fillvalue=None):
        super(PipelineMap, self).__init__()
        self.pipelines = []
        self.fillvalue = fillvalue
        self._iter = None
        self._pipeline_names = None

    def _begin(self):
        """
        tee the input into each sub pipeline and then
        zip the input iterators to create a single
        iterable before commencing iteration

        """
        iters = list(
            itertools.tee(self.input, len(self.pipelines))
        )
        for p in self.pipelines:
            p.chain(iters.pop())

        self._iter = itertools.izip_longest(
            *self.pipelines, fillvalue=self.fillvalue
        )
        self._pipeline_names = [p.label for p in self.pipelines]

    def next(self):
        """
        implement iteration by calling next on the izipped
        iterator and mapping results of each pipeline to the
        pipeline's label in the result dictionary
        """
        if self._iter is None:
            self._begin()

        value = dict(zip(self._pipeline_names, self._iter.next()))
        return value

    def add_pipeline(self, pipeline):
        """

        """
        if not isinstance(pipeline, Pipeline):
            msg = "Can only chain pipelines"
            raise RuntimeError(msg)

        self.pipelines.append(pipeline)

    def to_json(self):
        """
        _to_json_

        """
        result = {
            "type": type(self).__name__,
            "pipelines": [p.to_json() for p in self.pipelines],
        }
        if isinstance(self.input, PipelineOperator):
            result['input'] = self.input.to_json()
        return result
