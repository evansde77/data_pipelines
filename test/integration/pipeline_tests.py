#!/usr/bin/env python
"""
pipeline tests with fixtures

"""


import unittest
import data_pipelines.pipelines as p
import fixtures.math as m


class MathPipelineTests(unittest.TestCase):
    """simple linear pipeline tests using math fixtures"""

    def test_basic_pipeline(self):
        """test basic modifying pipeline"""
        square = p.PipelineTransform(action=m.square)

        double = p.PipelineTransform(action=m.double)
        double.chain(square)
        end = p.PipelineOperator(action=m.printer)
        end.chain(double)

        data = (x for x in range(10))
        pipeline = p.Pipeline(square, end)
        pipeline.chain(data)

        result = pipeline.execute()
        self.assertEqual(
            result, [0, 2, 8, 18, 32, 50, 72, 98, 128, 162]
        )

    def test_basic_filter_pipeline(self):
        """test a pipeline containing a filter"""
        even_filter = p.PipelineFilter(action=m.even)

        square = p.PipelineTransform(action=m.square)
        square.chain(even_filter)

        double = p.PipelineTransform(action=m.double)
        double.chain(square)
        end = p.PipelineOperator(action=m.printer)
        end.chain(double)

        data = (x for x in range(20))
        pipeline = p.Pipeline(even_filter, end)
        pipeline.chain(data)

        result = pipeline.execute()
        self.assertEqual(
            result,
            [2, 18, 50, 98, 162, 242, 338, 450, 578, 722]
        )

    def test_simple_map_pipeline(self):
        """test embedding a single linear pipeline in a map"""
        data = (x for x in range(10))
        sq = p.PipelineTransform(action=m.square)
        dbl = p.PipelineTransform(action=m.double)
        dbl.chain(sq)
        pipeline = p.Pipeline(sq, dbl, 'pipeline')

        pmap = p.PipelineMap()
        pmap.add_pipeline(pipeline)

        top_pipeline = p.Pipeline(pmap, pmap, 'top')
        top_pipeline.chain(data)

        result = top_pipeline.execute()
        values = []
        for x in result:
            self.failUnless('pipeline' in x)
            values.append(x['pipeline'])
        self.assertEqual(
            values, [0, 2, 8, 18, 32, 50, 72, 98, 128, 162]
        )




if __name__ == '__main__':
    unittest.main()
