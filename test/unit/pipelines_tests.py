#!/usr/bin/env python
"""
pipelines basic unit tests

"""
import mock
import unittest

from data_pipelines.pipelines import Pipeline


class PipelineUnittest(unittest.TestCase):
    """
    Tests for base Pipeline class

    """
    def test_pipeline_calls(self):
        """test basic method calls"""
        iter1 = range(10)
        iter2 = range(15)
        end_mock1 = mock.Mock()
        end_mock1.execute = mock.Mock()
        end_mock1.to_json = mock.Mock()
        end_mock1.to_json.return_value = {"mock1": "to_json"}

        end_mock2 = mock.Mock()
        end_mock2.execute = mock.Mock()
        end_mock2.to_json = mock.Mock()
        end_mock2.to_json.return_value = {"mock2": "to_json"}
        start_mock1 = mock.Mock()
        start_mock1.chain = mock.Mock()
        start_mock2 = mock.Mock()
        start_mock2.chain = mock.Mock()

        p1 = Pipeline(start_mock1, end_mock1)
        p1.chain(iter1)
        p2 = Pipeline(start_mock2, end_mock2, 'steve')
        p2.chain(iter2)

        self.failUnless(p1.label is not None)
        self.failUnless(p2.label == 'steve')

        p1.execute()
        p2.execute()
        self.failUnless(end_mock1.execute.called)
        self.failUnless(end_mock2.execute.called)
        self.failUnless(start_mock1.chain.called)
        self.failUnless(start_mock2.chain.called)

        json1 = p1.to_json()
        json2 = p2.to_json()
        self.assertEqual(json1['content'], end_mock1.to_json.return_value)
        self.assertEqual(json2['content'], end_mock2.to_json.return_value)
        self.assertEqual(json2['type'], 'Pipeline')
        self.assertEqual(json1['type'], 'Pipeline')
        self.assertEqual(json2['label'], 'steve')

if __name__ == '__main__':
    unittest.main()
