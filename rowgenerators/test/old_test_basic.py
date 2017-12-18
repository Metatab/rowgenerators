from __future__ import print_function

import platform
import unittest
from copy import deepcopy
from csv import DictReader, DictWriter

if False:
    from fs.tempfs import TempFS
    from rowgenerators.urls import Url


    from old.generators import get_generator
    from rowgenerators import RowGenerator
    from rowgenerators import SourceSpec
    from rowgenerators import parse_url_to_dict
    from rowgenerators import register_proto


    try:
        import fiona
        import shapely
        import pyproj
        geo_installed = True
    except ImportError:
        geo_installed = False


    try:
        import jupyter
        import nbconvert
        # This next import may fail due to version issues, but I'm not sure.
        # It seems to fail on Windows
        from nbconvert.exporters import get_exporter
        jupyter_installed = True
    except ImportError:
        jupyter_installed = False


    try:
        import metatab
        metatab_installed = True
    except ImportError:
        metatab_installed = False

    def data_path(v):
        from os.path import dirname, join
        d = dirname(__file__)
        return join(d, 'test_data', v)


    def script_path(v=None):
        from os.path import dirname, join
        d = dirname(__file__)
        if v is not None:
            return join(d, 'scripts', v)
        else:
            return join(d, 'scripts')


    def sources():
        import csv
        with open(data_path('sources.csv')) as f:
            r = csv.DictReader(f)
            return list(r)


    def cache_fs():
        from fs.tempfs import TempFS

        return TempFS('rowgenerator')


    class BasicTests(unittest.TestCase):
        def compare_dict(self, name, a, b):
            from rowgenerators.util import flatten
            fa = set('{}={}'.format(k, v) for k, v in flatten(a));
            fb = set('{}={}'.format(k, v) for k, v in flatten(b));

            # The declare lines move around a lot, and rarely indicate an error
            fa = {e for e in fa if not e.startswith('declare=')}
            fb = {e for e in fb if not e.startswith('declare=')}

            errors = len(fa - fb) + len(fb - fa)

            if errors:
                print("=== ERRORS for {} ===".format(name))

            if len(fa - fb):
                print("In a but not b")
                for e in sorted(fa - fb):
                    print('    ', e)

            if len(fb - fa):
                print("In b but not a")
                for e in sorted(fb - fa):
                    print('    ', e)

            self.assertEqual(0, errors)

        def test_source_spec_url(self):
            from rowgenerators import SourceSpec, RowGenerator
            from copy import deepcopy

            ss = SourceSpec(url='http://foobar.com/a/b.csv')
            self.assertEqual('b.csv', ss.target_file)
            self.assertIsNone(ss.target_segment)

            ss = SourceSpec(url='http://foobar.com/a/b.zip#a')
            print(ss._url)
            self.assertEqual('a', ss.target_file)
            self.assertIsNone(ss.target_segment)

            ss2 = deepcopy(ss)
            self.assertEqual(ss.target_file, ss2.target_file)
            self.assertIsNone(ss.target_segment)

            ss = SourceSpec(url='http://foobar.com/a/b.zip#a;b')
            self.assertEqual('a', ss.target_file)
            self.assertEqual('b', ss.target_segment)

            ss2 = deepcopy(ss)
            self.assertEqual(ss.target_file, ss2.target_file)
            self.assertEqual(ss.target_segment, ss2.target_segment)

            ss = RowGenerator(
                url='http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx')
            self.assertEqual('renter_cost_excel07.xlsx', ss.target_file)

            ss2 = deepcopy(ss)
            self.assertEqual(ss.target_file, ss2.target_file)

            for url in (
                    'http://example.com/foo/archive.zip',
                    'http://example.com/foo/archive.zip#file.xlsx',
                    'http://example.com/foo/archive.zip#file.xlsx;0',
                    'socrata+http://example.com/foo/archive.zip'
            ):
                pass

            print(SourceSpec(url='socrata+http://chhs.data.ca.gov/api/views/tthg-z4mf').__dict__)

        def test_run_sources(self):

            cache = cache_fs()

            for sd in sources():
                # Don't have the column map yet.
                if sd['name'] in ('simple_fixed', 'facilities'):
                    continue

                try:

                    ss = SourceSpec(**sd)

                    gen = RowGenerator(cache=cache, **sd)

                    rows = list(gen)

                    self.assertEquals(int(sd['n_rows']), len(rows))
                except Exception as e:
                    print('---')
                    print(sd['name'], e)
                    print(rows[0])
                    print(rows[-1])

        def test_inspect(self):

            from rowgenerators import inspect

            cache = cache_fs()

            us = 'http://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/CHS/Community%20Profiles/MCH_2010-2013.xlsx'

            ss = SourceSpec(us)

            print(ss.update(target_segment='foo'))
            print(ss._url.rebuild_url(target_segment='foo'))
            return

            for spec in inspect(ss, cache, callback=print):
                print(spec)

        def test_google(self):
            from rowgenerators import SourceSpec, GooglePublicSource
            spec = SourceSpec(url='gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w')

            self.assertEquals('gs', spec.proto)
            self.assertEquals('gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/', spec.url)
            self.assertEquals(
                'https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/export?format=csv',
                GooglePublicSource.download_url(spec))

            self.assertEquals(12004, len(list(spec.get_generator(cache_fs()))))

        def test_zip(self):

            from rowgenerators import enumerate_contents, RowGenerator, SourceError, TextEncodingError

            z = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip'
            cache = cache_fs()

            for c in enumerate_contents(z, cache):

                print(c.url, c.encoding)

                if c.target_format in ('foo', 'txt'):
                    continue

                gen = RowGenerator(url=c.url)
                try:
                    print(len(list(gen)))
                except (UnicodeDecodeError, TextEncodingError) as e:
                    print("UERROR", c.name, e)
                except SourceError as e:
                    print("ERROR", c.name, e)

        def test_d_and_c(self):
            from csv import DictReader
            from old.fetch import download_and_cache

            from os.path import isfile

            cache = TempFS()

            with open(data_path('sources.csv')) as f:
                for e in DictReader(f):
                    try:
                        d = download_and_cache(SourceSpec(**e), cache)
                    except ModuleNotFoundError:
                        # For when metatab isn't installed.
                        continue

                    self.assertTrue(isfile(d['sys_path']))

        def test_delayed_flo(self):
            from csv import DictReader

            cache = TempFS()
            success = []
            errors = []

            with open(data_path('sources.csv')) as f:
                for e in DictReader(f):

                    if e['name'] in ('simple_fixed',):
                        continue

                    if e['name'] not in ('zip_no_xls',):
                        continue

                    s = SourceSpec(**e)

                    print(s.dict)

                    d = get_generator(s, cache)

                    print(s._url, len(list(d)))

        def test_urls(self):

            headers = "in_url class url resource_url resource_file target_file scheme proto resource_format target_format " \
                      "is_archive encoding target_segment".split()

            import tempfile
            tf = tempfile.NamedTemporaryFile(prefix="rowgen", delete=False)
            temp_name = tf.name
            tf.close()

            # S3 URLS have these fields which need to be removed before writing to CSV files.
            def clean(do):

                for f in ['_orig_url', '_key', '_orig_kwargs', '_bucket_name']:
                    try:
                        del do[f]
                    except KeyError:
                        pass

            with open(data_path('url_classes.csv')) as f, open(temp_name, 'w') as f_out:
                w = None
                r = DictReader(f)
                errors = 0
                for i, d in enumerate(r):

                    url = d['in_url']

                    o = Url(url)

                    do = dict(o.__dict__.items())
                    del do['parts']

                    if w is None:
                        w = DictWriter(f_out, fieldnames=headers)
                        w.writeheader()
                    do['in_url'] = url
                    do['is_archive'] = o.is_archive
                    do['class'] = o.__class__.__name__
                    clean(do)
                    w.writerow(do)

                    d = {k: v if v else None for k, v in d.items()}
                    do = {k: str(v) if v else None for k, v in do.items()}  # str() turns True into 'True'

                    # a is the gague data from url_classes.csv
                    # b is the test object.

                    try:  # A, B
                        self.compare_dict(url, d, do)
                    except AssertionError as e:
                        errors += 1
                        print(e)
                        # raise

                self.assertEqual(0, errors)

            with open(data_path('url_classes.csv')) as f:

                r = DictReader(f)
                for i, d in enumerate(r):
                    u1 = Url(d['in_url'])

            with open(data_path('url_classes.csv')) as f:

                r = DictReader(f)
                for i, d in enumerate(r):
                    u1 = Url(d['in_url'])
                    d1 = u1.__dict__.copy()
                    d2 = deepcopy(u1).__dict__.copy()

                    # The parts will be different Bunch objects
                    clean(d1)
                    clean(d2)
                    del d1['parts']
                    del d2['parts']

                    self.assertEqual(d1, d2)

                    self.assertEqual(d1, u1.dict)

            for us in ("http://example.com/foo.zip", "http://example.com/foo.zip#a;b"):
                u = Url(us, encoding='utf-8')
                u2 = u.update(target_file='bingo.xls', target_segment='1')

                self.assertEqual('utf-8', u2.dict['encoding'])
                self.assertEqual('bingo.xls', u2.dict['target_file'])
                self.assertEqual('1', u2.dict['target_segment'])

        def test_url_update(self):

            u1 = Url('http://example.com/foo.zip')

            self.assertEqual('http://example.com/foo.zip#bar.xls', u1.rebuild_url(target_file='bar.xls'))
            self.assertEqual('http://example.com/foo.zip#0', u1.rebuild_url(target_segment=0))
            self.assertEqual('http://example.com/foo.zip#bar.xls%3B0',
                             u1.rebuild_url(target_file='bar.xls', target_segment=0))

            u2 = u1.update(target_file='bar.xls')

            self.assertEqual('bar.xls', u2.target_file)
            self.assertEqual('xls', u2.target_format)

            self.assertEqual('http://example.com/foo.zip', u1.rebuild_url(False, False))

            self.assertEqual('file:metatadata.csv', Url('file:metatadata.csv').rebuild_url())

        def test_parse_file_urls(self):
            from rowgenerators.util import parse_url_to_dict, unparse_url_dict
            urls = [
                ('file:foo/bar/baz', 'foo/bar/baz', 'file:foo/bar/baz'),
                ('file:/foo/bar/baz', '/foo/bar/baz', 'file:/foo/bar/baz'),
                ('file://example.com/foo/bar/baz', '/foo/bar/baz', 'file://example.com/foo/bar/baz'),
                ('file:///foo/bar/baz', '/foo/bar/baz', 'file:/foo/bar/baz'),
            ]

            for i, o, u in urls:
                p = parse_url_to_dict(i)
                self.assertEqual(o, p['path'])
                self.assertEqual(u, unparse_url_dict(p))
                # self.assertEqual(o, parse_url_to_dict(u)['path'])



        def test_metatab_url(self):

            urlstr = 'metatab+http://s3.amazonaws.com/library.metatab.org/cdss.ca.gov-residential_care_facilities-2017-ca-7.csv#facilities'

            u = Url(urlstr)

            self.assertEqual('http', u.scheme)
            self.assertEqual('metatab', u.proto)
            self.assertEqual('http://s3.amazonaws.com/library.metatab.org/cdss.ca.gov-residential_care_facilities-2017-ca-7.csv', u.resource_url)
            self.assertEqual('cdss.ca.gov-residential_care_facilities-2017-ca-7.csv', u.target_file)
            self.assertEqual('facilities', u.target_segment)

        @unittest.skipIf(not metatab_installed, "Metatab modules are not installed")
        def test_metapack(self):

            from metatab import open_package, resolve_package_metadata_url

            cache = cache_fs()

            url = 'metatab+http://library.metatab.org/example.com-simple_example-2017-us-1#random-names'

            rg = RowGenerator(cache=cache, url=url)

            package_url, metadata_url = resolve_package_metadata_url(rg.generator.spec.resource_url)

            self.assertEquals('http://library.metatab.org/example.com-simple_example-2017-us-1/',package_url)
            self.assertEquals('http://library.metatab.org/example.com-simple_example-2017-us-1/metadata.csv',metadata_url)

            doc = open_package(rg.generator.spec.resource_url, cache=cache)

            self.assertEquals('http://library.metatab.org/example.com-simple_example-2017-us-1/data/random-names.csv',
                              doc.resource('random-names').resolved_url)


            urls = ['metatab+http://library.metatab.org/example.com-simple_example-2017-us-1#random-names',
                    'metatab+http://library.metatab.org/example.com-simple_example-2017-us-1.zip#random-names',
                    'metatab+http://library.metatab.org/example.com-simple_example-2017-us-1.xlsx#random-names'
                    ]

            for url in urls:
                gen = None
                try:
                    gen = RowGenerator(cache=cache, url=url)

                    rows = list(gen)

                    self.assertEquals(101, len(rows))
                except:
                    print("ERROR URL", url)
                    print("Row Generator ", gen)
                    raise

        @unittest.skipIf(platform.system() == 'Windows','ProgramSources don\'t work on Windows')
        def test_program(self):

            urls = (
                ('program:rowgen.py', 'rowgen.py'),
                ('program:/rowgen.py', '/rowgen.py'),
                ('program:///rowgen.py', '/rowgen.py'),
                ('program:/a/b/c/rowgen.py', '/a/b/c/rowgen.py'),
                ('program:/a/b/c/rowgen.py', '/a/b/c/rowgen.py'),
                ('program:a/b/c/rowgen.py', 'a/b/c/rowgen.py'),
                ('program+http://foobar.com/a/b/c/rowgen.py', '/a/b/c/rowgen.py'),
            )

            for u, v in urls:
                url = Url(u)

                self.assertEquals(url.path, v, u)

            cache = cache_fs()

            options = {
                '-a': 'a',
                '-b': 'b',
                '--foo': 'foo',
                '--bar': 'bar'
            }

            options.update({'ENV1': 'env1', 'ENV2': 'env2', 'prop1': 'prop1', 'prop2': 'prop2'})

            gen = RowGenerator(cache=cache, url='program:rowgen.py', working_dir=script_path(),
                               generator_args=options)

            rows = list(gen)

            for row in rows:
                print(row)

        @unittest.skipIf(not jupyter_installed, "Juptyer notebook modules are not installed")
        def test_notebook(self):

            urls = (
                'ipynb+file:foobar.ipynb',
                'ipynb+http://example.com/foobar.ipynb',
                'ipynb:foobar.ipynb'

            )

            for url in urls:
                u = Url(url)
                print(u, u.path, u.resource_url)

                s = SourceSpec(url)
                print(s, s.proto, s.scheme, s.resource_url, s.target_file, s.target_format)
                self.assertIn(s.scheme, ('file', 'http'))
                self.assertEquals('ipynb', s.proto)
                # print(download_and_cache(s, cache_fs()))

            gen = RowGenerator(cache=cache_fs(),
                               url='ipynb:Py3Notebook.ipynb#lst',
                               working_dir=script_path(),
                               generator_args={'mult': lambda x: x * 3})

            print(gen.generator.execute())

        @unittest.skipIf(not geo_installed,"geo modules are not installed")
        def test_shapefile(self):

            url = "shape+http://s3.amazonaws.com/test.library.civicknowledge.com/census/tl_2016_us_state.geojson"

            gen = RowGenerator(url=url, cache=cache_fs())

            self.assertTrue(gen.is_geo)

            print("HEADERS", gen.headers)

            x = 0
            for row in gen.iter_rp():
                x += float(row['INTPTLON'])

            self.assertEquals(-4776, int(x))

            url = "shape+http://s3.amazonaws.com/test.library.civicknowledge.com/census/tl_2016_us_state.geojson.zip"

            gen = RowGenerator(url=url, cache=cache_fs())

            self.assertTrue(gen.is_geo)

            x = 0
            for row in gen.iter_rp():
                x += float(row['INTPTLON'])

            self.assertEquals(-4776, int(x))

            return

            url = "shape+http://s3.amazonaws.com/test.library.civicknowledge.com/census/tl_2016_us_state.zip"

            gen = RowGenerator(url=url, cache=cache_fs())

            for row in gen:
                print(row)




        def test_windows_urls(self):

            url = 'w:/metatab36/metatab-py/metatab/templates/metatab.csv'

            print(parse_url_to_dict(url))

            url = 'N:/Desktop/metadata.csv#renter_cost'

            print(parse_url_to_dict(url))

        def test_query_urls(self):

            url='https://s3.amazonaws.com/private.library.civicknowledge.com/civicknowledge.com-rcfe_health-1/metadata.csv?AWSAccessKeyId=AKIAJFW23EPQCLXRU7DA&Signature=A39XhRP%2FTKAxv%2B%2F5vCubwWPDag0%3D&Expires=1494223447'

            u = Url(url)

            print(u.resource_file, u.resource_format)
            print(u.target_file, u.target_format)


        def test_s3_url(self):

            from rowgenerators.urls import S3Url

            url_str = 's3://bucket/a/b/c/file.csv'

            u = Url(url_str)

            self.assertEquals(S3Url, type(u))


        def test_register(self):

            from pandasreporter import CensusReporterSource, get_cache

            register_proto('censusreporter', CensusReporterSource)

            url = 'censusreporter:B17001/140/05000US06073'

            gen = RowGenerator(cache=get_cache(), url=url)

            self.assertEquals('B17001', gen.generator.table_id )
            self.assertEquals('140', gen.generator.summary_level)
            self.assertEquals('05000US06073', gen.generator.geoid)

            for row in gen:
                print(row)


    if __name__ == '__main__':
        unittest.main()
