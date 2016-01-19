#!/usr/bin/env python
"""
serialization tests
"""

import unittest
import data_pipelines.pipelines as p
import fixtures.math as m


class PipelineSerializationTests(unittest.TestCase):
    """test serializing/deserializing various chains"""

    def test_basic_pipeline(self):
        """test serializing simple pipeline"""
        square = p.PipelineTransform(action=m.square)

        double = p.PipelineTransform(action=m.double)
        double.chain(square)
        end = p.PipelineOperator(action=m.printer)
        end.chain(double)

        data = (x for x in range(10))
        pipeline = p.Pipeline(square, end)
        pipeline.chain(data)

        pipeline_json = pipeline.to_json()

        result = pipeline.execute()
        self.assertEqual(
            result, [0, 2, 8, 18, 32, 50, 72, 98, 128, 162]
        )

        pipeline2 = p.Pipeline.from_configuration(pipeline_json)
        data2 = (x for x in range(10))
        pipeline2.chain(data2)
        result2 = pipeline2.execute()
        self.assertEqual(
            result2, [0, 2, 8, 18, 32, 50, 72, 98, 128, 162]
        )

    def test_pipeline_map(self):
        """test with a pipeline containing a map"""
        data1 = (x for x in range(10))
        data2 = (x for x in range(10))

        sq = p.PipelineTransform(action=m.square)
        dbl = p.PipelineTransform(action=m.double)
        dbl.chain(sq)
        pipeline = p.Pipeline(sq, dbl, 'pipeline')

        pmap = p.PipelineMap()
        pmap.add_pipeline(pipeline)

        top_pipeline = p.Pipeline(pmap, pmap, 'top')
        top_pipeline.chain(data1)

        top_json = top_pipeline.to_json()
        pipeline2 = p.Pipeline.from_configuration(top_json)
        pipeline2.chain(data2)

        result1 = top_pipeline.execute()
        result2 = pipeline2.execute()
        values = []
        for x in result1:
            self.failUnless('pipeline' in x)
            values.append(x['pipeline'])
        self.assertEqual(
            values, [0, 2, 8, 18, 32, 50, 72, 98, 128, 162]
        )

        self.assertEqual(result1, result2)


    def test_complex_pipeline_map(self):
        """test more complicated map contents"""



if __name__ == '__main__':
    unittest.main()
