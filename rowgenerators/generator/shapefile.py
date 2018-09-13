# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """


from functools import partial
from itertools import islice

from rowgenerators.appurl.file.shapefile import ShapefileUrl, ShapefileShpUrl
from rowgenerators.source import Source
from rowgenerators.exceptions import RowGeneratorError

# Looks like PyPy doesn't have ModuleNotFoundError

try:
    ModuleNotFoundError
except NameError:
    class ModuleNotFoundError(ImportError):
        pass


def _import_requirements():
    """Check that the requirements are available. These are in a seperate function because test collectors will
    import the file looking for tests, causing a failure, even if no geo tests are run"""
    try:
        import fiona
        from fiona.crs import from_epsg
        from shapely.geometry import asShape
        from shapely.ops import transform
        import pyproj
    except (ModuleNotFoundError, ImportError) as e:
        raise ImportError("Using ShapefileSource requires installing fiona, shapely and pyproj ") from e
        pass  # HACK Because this file gets collected by the test collectors

class GeoSourceBase(Source):
    """ Base class for all geo sources. """
    pass


class ShapefileSource(GeoSourceBase):
    """ Accessor for shapefiles (*.shp) with geo data. """

    def __init__(self, url, cache=None, working_dir=None, env=None, **kwargs):
        """
        A row source for shapefiles.

        By default will try to re-project to epsg:4326 during iteration. This can be turned off by setting
        the 'projection' argument to '<source>'

        :param url:
        :param cache:
        :param working_dir:
        :param env:
        :param projection: Either an EPSG string, defaults to 'epsg:4326', or '<source>' to not project
        :param kwargs:
        """

        super().__init__(url, cache, working_dir)

        _import_requirements()

        assert isinstance(url,(ShapefileUrl, ShapefileShpUrl))

        self.property_schema = self._parameters

        self._kwargs = kwargs

        target_projection = (env or {}).get('projection', 'epsg:4326').lower()

        try:
            int(target_projection)
            target_projection = 'epsg:{}'.format(target_projection)
        except ValueError:
            if not target_projection.startswith('epsg:') and target_projection != '<source>':
                raise RowGeneratorError("ShapefileSource projection property must start with 'epsg:'  or be an integer")

        self.target_projection = target_projection
        self.source_projection = None

        # Holds metadata, such as EPSG, that is inferred during processing.
        self._meta = {}

    def _convert_column(self, shapefile_column):
        """ Converts column from a *.shp file to the column expected by ambry_sources."""
        name, type_ = shapefile_column
        type_ = type_.split(':')[0]
        return {'name': name, 'type': type_}

    @property
    def columns(self):
        """ Returns columns for the file accessed by accessor.

        """
        #
        # first column is id and will contain id of the shape.
        columns = [{'name': 'id', 'type': 'int'}]

        # extend with *.shp file columns converted to ambry_sources format.
        columns.extend(list(map(self._convert_column, self.property_schema.items())))

        # last column is wkt value.
        columns.append({'name': 'geometry', 'type': 'geometry_type'})
        return columns

    @property
    def headers(self):
        """Return headers. This must be run after iteration, since the value that is returned is
        set in iteration """

        # self.spec.columns = [c for c in self._get_columns(property_schema)]

        return [x['name'] for x in self.columns]

    @property
    def meta(self):
        return self._meta

    def _open_file_params(self):
        from zipfile import ZipFile

        layer_index = self.ref.target_segment or 0

        if self.ref.resource_format == 'zip':
            # Find the SHP file. I thought Fiona used to do this itself ...
            assert self.ref.target_file

            vfs = 'zip://{}'.format(self.ref.fspath)

            if self.ref.target_file:
                shp_file = '/' + self.ref.target_file.strip('/')
            else:
                shp_file = '/' + next(
                    n for n in ZipFile(self.ref.fspath).namelist() if (n.endswith('.shp') or n.endswith('geojson')))
        else:
            shp_file = self.ref.fspath
            vfs = None

        return vfs, shp_file, layer_index

    @property
    def _parameters(self):
        import fiona

        vfs, shp_file, layer_index = self._open_file_params()

        with fiona.open(shp_file, vfs=vfs, layer=layer_index) as source:

            return source.schema['properties']


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
        from shapely.ops import transform
        import pyproj

        self.start()

        vfs, shp_file, layer_index = self._open_file_params()

        with fiona.open(shp_file, vfs=vfs, layer=layer_index) as source:

            if self.target_projection == '<source>':
                self.target_projection = source.crs.get('init')

            self.source_projection = source.crs.get('init')

            if self.source_projection  != self.target_projection:

                projection_type, self.epsg_propjection_code = self.target_projection.split(':')

                assert projection_type == 'epsg'

                int(self.epsg_propjection_code)

                project = partial(pyproj.transform,
                                  pyproj.Proj(source.crs, preserve_units=True),
                                  pyproj.Proj(from_epsg(str(self.epsg_propjection_code)))
                                  )

                self.projection = self.target_projection
            else:
                project = None
                self.projection = self.source_projection

            self._meta['source_projection'] = self.source_projection
            self._meta['target_projection'] = self.target_projection
            self._meta['projection'] = self.projection

            yield self.headers


            for i,s in enumerate(source):

                row_data = s['properties']
                shp = asShape(s['geometry'])

                row = [int(s['id'])]
                for col_name, elem in row_data.items():
                    row.append(elem)

                if project:
                    row.append(transform(project, shp))

                else:
                    row.append(shp)

                yield row

        self.finish()


    def dataframe(self, limit=None):
        """An alias for geoframe()"""

        return self.geoframe()



    def geoframe(self):
        """Return a geopandas dataframe. The geoframe does not reproject, ( which is a lot faster )
        but does set the crs with the actual projection, so you can re-project with to_crs()

        """
        import geopandas as gpd

        vfs, shp_file, layer_index = self._open_file_params()

        return  gpd.read_file(shp_file, vfs=vfs, layer=layer_index)


class GeoJsonSource(Source):
    """Generate rows, of Shapeley objects, from a GeoJson file reference"""

    delimiter = ','

    def __init__(self, ref, cache=None, working_dir=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.url = ref

    def __iter__(self):
        """Iterate over all of the lines in the file"""
        import json

        t = self.url.get_resource().get_target()

        gj = json.loads(t.read())


        s = shape(gj['geometry'])