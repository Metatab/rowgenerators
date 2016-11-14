# -*- coding: utf-8 -*-
import unittest
from collections import OrderedDict

from attrdict import AttrDict

try:
    # py3
    from unittest.mock import Mock, MagicMock, patch, call, PropertyMock
except ImportError:
    # py2
    from mock import Mock, MagicMock, patch, call, PropertyMock

from six import u

from ambry_sources.sources import SourceSpec, ShapefileSource, DatabaseRelationSource
from ambry_sources.sources.util import DelayedOpen


class TestShapefileSource(unittest.TestCase):

    def _get_fake_collection(self):
        """ Returns fake collection which could be used as replacement for fiona.open(...) return value. """

        class FakeCollection(object):
            schema = {
                'properties': OrderedDict([('col1', 'int:10')])}

            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                pass

            def __iter__(self):
                return iter([{'properties': OrderedDict([('col1', 1)]), 'geometry': 'LINE', 'id': '0'}])

        return FakeCollection()

    # _convert_column tests
    def test_converts_shapefile_column(self):
        spec = Mock()
        spec.start_line = 0
        spec.header_lines = []
        fstor = Mock(DelayedOpen)
        source = ShapefileSource(spec, fstor)
        expected_column = {'name': 'name1', 'type': 'int'}
        self.assertEqual(
            source._convert_column((u('name1'), 'int:3')),
            expected_column)

    # _get_columns tests
    def test_converts_given_columns(self):
        spec = Mock()
        spec.start_line = 0
        spec.header_lines = []
        fstor = Mock(spec=DelayedOpen)
        source = ShapefileSource(spec, fstor)
        column1 = ('name1', 'int:10')
        column2 = ('name2', 'str:10')
        converted_column1 = {'name': 'name1', 'type': 'int'}
        converted_column2 = {'name': 'name2', 'type': 'str'}
        shapefile_columns = OrderedDict([column1, column2])
        ret = source._get_columns(shapefile_columns)
        self.assertIn(converted_column1, ret)
        self.assertIn(converted_column2, ret)

    def test_extends_with_id_and_geometry(self):
        spec = Mock()
        spec.start_line = 0
        spec.header_lines = []
        fstor = Mock(spec=DelayedOpen)
        source = ShapefileSource(spec, fstor)
        shapefile_columns = OrderedDict()
        ret = source._get_columns(shapefile_columns)
        self.assertEqual(len(ret), 2)
        names = [x['name'] for x in ret]
        self.assertIn('id', names)
        self.assertIn('geometry', names)

        types = [x['type'] for x in ret]
        self.assertIn('geometry_type', types)

    @patch('shapely.wkt.dumps')
    @patch('shapely.geometry.shape')
    @patch('fiona.open')
    def test_reads_first_layer_if_spec_segment_is_empty(self, fake_open, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.return_value = fake_collection

        spec = SourceSpec('http://example.com')
        assert spec.segment is None
        fstor = Mock(spec=DelayedOpen)
        fstor._fs = Mock()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())
        self.assertEqual(len(fake_open.mock_calls), 1)
        self.assertEqual(
            fake_open.call_args_list[0][1]['layer'],
            0,
            'open function was called with wrong layer.')

    @patch('shapely.wkt.dumps')
    @patch('shapely.geometry.shape')
    @patch('fiona.open')
    def test_reads_layer_specified_by_segment(self, fake_open, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.return_value = fake_collection
        spec = SourceSpec('http://example.com', segment=5)
        fstor = Mock(spec=DelayedOpen)
        fstor._fs = Mock()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())
        self.assertEqual(len(fake_open.mock_calls), 1)
        self.assertEqual(
            fake_open.call_args_list[0][1]['layer'],
            5,
            'open function was called with wrong layer.')

    @patch('shapely.wkt.dumps')
    @patch('shapely.geometry.shape')
    @patch('ambry_sources.sources.accessors.ShapefileSource._get_columns')
    @patch('fiona.open')
    def test_populates_columns_of_the_spec(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.return_value = fake_collection
        fake_get.return_value = [{'name': 'col1', 'type': 'int'}]
        spec = SourceSpec('http://example.com')
        fstor = Mock(spec=DelayedOpen)
        fstor._fs = Mock()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())
        self.assertEqual(len(source.spec.columns), 1)
        self.assertEqual(source.spec.columns[0].name, 'col1')
        self.assertEqual(len(fake_open.mock_calls), 1)
        self.assertEqual(len(fake_get.mock_calls), 2)

    @patch('shapely.wkt.dumps')
    @patch('shapely.geometry.shape')
    @patch('ambry_sources.sources.accessors.ShapefileSource._get_columns')
    @patch('fiona.open')
    def test_converts_row_id_to_integer(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.return_value = fake_collection
        fake_shape.expects_call().is_a_stub()
        fake_dumps.expects_call().is_a_stub()
        fake_get.return_value = [{'name': 'col1', 'type': 'int'}]
        spec = SourceSpec('http://example.com')
        fstor = Mock(spec=DelayedOpen)
        fstor._fs = Mock()
        source = ShapefileSource(spec, fstor)
        row_gen = source._get_row_gen()
        first_row = next(row_gen)
        self.assertEqual(first_row[0], 0)
        self.assertEqual(len(fake_open.mock_calls), 1)
        self.assertEqual(len(fake_get.mock_calls), 2)

    @patch('shapely.wkt.dumps')
    @patch('shapely.geometry.shape')
    @patch('ambry_sources.sources.accessors.ShapefileSource._get_columns')
    @patch('fiona.open')
    def test_saves_header(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.return_value = fake_collection
        fake_get.return_value = [
            {'name': 'id', 'type': 'int'},
            {'name': 'col1', 'type': 'int'},
            {'name': 'geometry', 'type': 'geometry_type'}]
        spec = SourceSpec('http://example.com')
        fstor = Mock(spec=DelayedOpen)
        fstor._fs = Mock()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())
        self.assertEqual(source._headers, ['id', 'col1', 'geometry'])
        self.assertEqual(len(fake_open.mock_calls), 1)
        self.assertEqual(len(fake_get.mock_calls), 2)

    @patch('shapely.wkt.dumps')
    @patch('shapely.geometry.shape')
    @patch('ambry_sources.sources.accessors.ShapefileSource._get_columns')
    @patch('fiona.open')
    def test_last_element_in_the_row_is_wkt(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.return_value = fake_collection
        fake_shape.expects_call().is_a_stub()
        fake_dumps.return_value = 'I AM FAKE WKT'
        fake_get.return_value = [{'name': 'col1', 'type': 'int'}]
        spec = SourceSpec('http://example.com')
        fstor = Mock(spec=DelayedOpen)
        fstor._fs = Mock()
        source = ShapefileSource(spec, fstor)
        row_gen = source._get_row_gen()
        first_row = next(row_gen)
        self.assertEqual(first_row[-1], 'I AM FAKE WKT')
        self.assertEqual(len(fake_open.mock_calls), 1)
        self.assertEqual(len(fake_get.mock_calls), 2)


class DatabaseRelationSourceTest(unittest.TestCase):

    def test_uses_url_as_table(self):
        fake_execute = Mock(return_value=iter([[1], [2]]))
        connection = AttrDict({'execute': fake_execute})
        spec = SourceSpec('table1')
        relation_source = DatabaseRelationSource(spec, 'sqlite', connection)
        rows = [x for x in relation_source]
        self.assertEqual(rows, [[1], [2]])
        fake_execute.assert_called_once_with('SELECT * FROM {};'.format('table1'))
