# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.source import Source

class GeoSourceBase(Source):
    """ Base class for all geo sources. """
    pass


class ShapefileSource(GeoSourceBase):
    """ Accessor for shapefiles (*.shp) with geo data. """

    def __init__(self, url, cache=None, working_dir=None):
        super().__init__(url, cache, working_dir)



    def _convert_column(self, shapefile_column):
        """ Converts column from a *.shp file to the column expected by ambry_sources.

        Args:
            shapefile_column (tuple): first element is name, second is type.

        Returns:
            dict: column spec as ambry_sources expects

        Example:
            self._convert_column((u'POSTID', 'str:20')) -> {'name': u'POSTID', 'type': 'str'}

        """
        name, type_ = shapefile_column
        type_ = type_.split(':')[0]
        return {'name': name, 'type': type_}

    def _get_columns(self, shapefile_columns):
        """ Returns columns for the file accessed by accessor.

        Args:
            shapefile_columns (SortedDict): key is column name, value is column type.

        Returns:
            list: list of columns in ambry_sources format

        Example:
            self._get_columns(SortedDict((u'POSTID', 'str:20'))) -> [{'name': u'POSTID', 'type': 'str'}]

        """
        #
        # first column is id and will contain id of the shape.
        columns = [{'name': 'id', 'type': 'int'}]

        # extend with *.shp file columns converted to ambry_sources format.
        columns.extend(list(map(self._convert_column, iter(shapefile_columns.items()))))

        # last column is wkt value.
        columns.append({'name': 'geometry', 'type': 'geometry_type'})
        return columns

    @property
    def headers(self):
        """Return headers. This must be run after iteration, since the value that is returned is
        set in iteration """

        return list(self._headers)

    def __iter__(self):
        """ Returns generator over shapefile rows.

        Note:
            The first column is an id field, taken from the id value of each shape
            The middle values are taken from the property_schema
            The last column is a string named geometry, which has the wkt value, the type is geometry_type.

        """

        # These imports are nere, not at the module level, so the geo
        # support can be an extra

        import fiona
        from fiona.crs import from_epsg
        from shapely.geometry import asShape
        from zipfile import ZipFile
        from shapely.ops import transform
        import pyproj
        from functools import partial

        layer_index = self.spec.target_segment or 0

        if self.spec.resource_format == 'zip':
            # Find the SHP file. I thought Fiona used to do this itself ...
            shp_file = '/'+next(n for n in ZipFile(self.syspath).namelist() if (n.endswith('.shp') or n.endswith('geojson')))
            vfs = 'zip://{}'.format(self.syspath)
        else:
            shp_file = self.syspath
            vfs = None

        self.start()

        with fiona.open(shp_file, vfs=vfs, layer=layer_index) as source:

            if source.crs.get('init') != 'epsg:4326':
                # Project back to WGS84

                project = partial(pyproj.transform,
                                  pyproj.Proj(source.crs, preserve_units=True),
                                  pyproj.Proj(from_epsg('4326'))
                                  )

            else:
                project = None

            property_schema = source.schema['properties']

            self.spec.columns = [c for c in self._get_columns(property_schema)]
            self._headers = [x['name'] for x in self._get_columns(property_schema)]

            yield self.headers

            for i,s in enumerate(source):

                row_data = s['properties']
                shp = asShape(s['geometry'])

                row = [int(s['id'])]
                for col_name, elem in six.iteritems(row_data):
                    row.append(elem)

                if project:
                    row.append(transform(project, shp))

                else:
                    row.append(shp)

                yield row

        self.finish()

