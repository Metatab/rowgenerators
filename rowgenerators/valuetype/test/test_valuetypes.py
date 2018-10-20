# -*- coding: utf-8 -*-



import unittest

from rowgenerators.rowpipe import Column
from rowgenerators.exceptions import ConfigurationError


def ct(t):

    col = Column('name', datatype='str',  transform=t)

    segs = col.expanded_transform

    o = []

    for seg in segs:


        seg_str = []

        if seg.init:
            try:
                seg_str.append('^' + seg.init)

            except TypeError:
                pass

        if seg.transforms:
            seg_str += seg.transforms

        if seg.exception:
            seg_str.append('!' + seg.exception)

        o.append('|'.join(seg_str))

    return ';'.join(o)

class Test(unittest.TestCase):

    def test_clean_transform(self):


        self.assertEqual('^init;t1|t2|t3|t4|!except',
                         ct('^init;t1|t2|t3|t4|!except'))

        self.assertEqual('^init;t1|t2|t3|t4|!except1;t1|t2|t3|t4|!except2',
                         ct('t1|^init|t2|!except1|t3|t4;t1|t2|!except2|t3|t4'))

        self.assertEqual('^init;t1|t2|t3|t4|!except;t4',
                         ct('t1|^init|t2|!except|t3|t4;t4'))

        self.assertEqual('^init;t1|t2|t3|t4|!except',
                         ct('t1|^init|t2|!except|t3|t4;;'))

        self.assertEqual('^init;t1|t2|t3|t4|!except',
                         ct('|t1|^init|t2|!except|t3|t4;;'))

        self.assertEqual('^init', ct('^init'))

        self.assertEqual(';!except', ct('!except'))

        self.assertEqual(ct(';transform2'), ';transform2')


        with self.assertRaises(ConfigurationError):  # Two inits in a segment
            ct('t1|^init|t2|^init|!except|t3|t4')

        c = Column(name='column', datatype='int')

        c.transform = 't1|^init|t2|!except|t3|t4'

        #self.assertEqual(['init'], [e['init'] for e in c.expanded_transform])
        #self.assertEqual([['t1', 't2', 't3', 't4']], [e['transforms'] for e in c.expanded_transform])

    def test_raceeth(self):

        return

        self.assertEqual(1, RaceEthNameHCI('AIAN').civick)
        self.assertEqual('aian', RaceEthNameHCI('AIAN').civick.name)
        self.assertEqual(6, RaceEthNameHCI('White').civick)
        self.assertEqual('white', RaceEthNameHCI('White').civick.name)

        self.assertEqual('all', RaceEthNameHCI('Total').civick.name)

        self.assertFalse(bool(RaceEthNameHCI(None).civick.name))


    def test_text(self):

        from rowgenerators.valuetype import TextValue, cast_str

        x = cast_str(TextValue(None), 'foobar', {})
        self.assertEqual(None, x)

        print (cast_str(TextValue(None), 'foobar', {}))

    def test_time(self):

        from rowgenerators.valuetype import  IntervalIsoVT, YearValue, YearRangeValue, resolve_value_type
        from rowgenerators.valuetype import DateValue, TimeValue

        self.assertEqual(2000, YearValue('2000'))

        self.assertFalse(bool(YearValue('2000-2001')))

        self.assertEqual('2000/2001', str(YearRangeValue('2000-2001')))
        self.assertEqual(2000, YearRangeValue('2000-2001').start)
        self.assertEqual(2001, YearRangeValue('2000-2001').end)

        self.assertEqual('2000/2001', str(YearRangeValue('2000/2001')))

        self.assertEqual('1981-04-05/1981-03-06',str(IntervalIsoVT('P1M/1981-04-05')))

        self.assertEqual(4, DateValue('1981-04-05').month)

        self.assertEqual(34,TimeValue('12:34').minute)

        i = resolve_value_type('interval')('2000-2001')
        i.raise_for_error()
        self.assertEqual(2000, i.start.year)
        self.assertEqual(2001, i.end.year)

        i = resolve_value_type('interval')('2000')
        i.raise_for_error()
        self.assertEqual(2000, i.start.year)
        self.assertEqual(2001, i.end.year)

        i = resolve_value_type('interval')(2000)
        i.raise_for_error()
        self.assertEqual(2000, i.start.year)
        self.assertEqual(2001, i.end.year)

        i = resolve_value_type('interval')(' 2000 ')
        i.raise_for_error()
        self.assertEqual(2000, i.start.year)
        self.assertEqual(2001, i.end.year)

        with self.assertRaises(ValueError):
            i = resolve_value_type('interval')(' foobar ')

        i = resolve_value_type('interval')(2010.0)
        i.raise_for_error()
        self.assertEqual(2010, i.start.year)
        self.assertEqual(2011, i.end.year)


    def test_geo(self):

        from rowgenerators.valuetype import GeoAcs, GeoidGvid, resolve_value_type, cast_unicode
        from geoid import acs

        # Check the ACS Geoid directly
        self.assertEqual('California', acs.State(6).geo_name)
        self.assertEqual('San Diego County, California', acs.County(6,73).geo_name)
        self.assertEqual('place in California', acs.Place(6,2980).geo_name)

        # THen check via parsing through the GeoAcsVT
        self.assertEqual('California', GeoAcs(str(acs.State(6))).geo_name)
        self.assertEqual('San Diego County, California', GeoAcs(str(acs.County(6, 73))).geo_name)
        self.assertEqual('place in California', GeoAcs(str(acs.Place(6, 2980))).geo_name)

        self.assertEqual('California', GeoidGvid('0O0601').state_name)
        self.assertEqual('Alameda County, California',  resolve_value_type('gvid')('0O0601').acs.geo_name)

        # Check that adding a parameter to the vt code will select a new parser.
        cls = resolve_value_type('geoid/census/tract')

        self.assertTrue(bool(cls))

        self.assertEqual(402600, cls('06001402600').tract)

        self.assertEqual('4026.00', cls('06001402600').dotted)

        self.assertEqual('4002.00',cast_unicode(cls('06001400200').dotted, 'tract', {}))

    def test_numbers(self):
        from collections import defaultdict
        from rowgenerators.valuetype import FloatDimension, cast_float

        errors = defaultdict(set)

        f = FloatDimension(100.4)
        f.raise_for_error()
        self.assertFalse(f.is_none)
        self.assertEqual(100.4, f)

        self.assertEqual(100.4, cast_float(f, 'test',errors))

        self.assertEqual(0, len(errors))

        f = FloatDimension('')
        f.raise_for_error()
        self.assertTrue(f.is_none)

        print(f, type(f))
        print(cast_float(f, 'test', errors))
        print(cast_float(f, 'test', errors))

        self.assertEqual(0, len(errors))

        f = FloatDimension('a')
        with self.assertRaises(ValueError):
            f.raise_for_error()

        self.assertFalse(f.is_none)

        print(f, type(f))
        print(cast_float(f, 'test', errors))
        print(cast_float(f, 'test', errors))

        print(errors)

    def test_failures(self):

        from rowgenerators.valuetype import StrValue, upper, NoneValue

        v = StrValue(None)

        self.assertIs(v, NoneValue)

        v = upper(v)

        self.assertIs(v, NoneValue)


    def test_measures_errors(self):

        import rowgenerators.valuetype as vt
        from rowgenerators.valuetype import resolve_value_type

        self.assertEqual('A standard error', vt.StandardErrorVT.__doc__)

        self.assertEqual( vt.ConfidenceIntervalHalfVT, resolve_value_type('ci'))

        # Test on-the-fly classes. The class is returned for e/ci, but it created a new class
        # and the vt_code is set to e/ci/u/95
        t = resolve_value_type('ci/u/95')
        print(t)
        self.assertIsNotNone(t)
        self.assertEqual(vt.ConfidenceIntervalHalfVT, resolve_value_type('ci'))
        self.assertEqual(12.34, float(t(12.34)))
        self.assertEqual('ci/u/95', t(12.34).vt_code)

        t = resolve_value_type('margin/90')
        self.assertEqual(12.34, float(t(12.34)))
        self.assertEqual('margin/90', t(12.34).vt_code)

        self.assertAlmostEqual(10.0, resolve_value_type('margin/90')(16.45).se)
        self.assertAlmostEqual(10.0, resolve_value_type('margin/95')(19.6).se)
        self.assertAlmostEqual(10.0, resolve_value_type('margin/99')(25.75).se)

        # Convert to various margins.
        v = resolve_value_type('se')(10)
        self.assertEqual(10, int(v))
        self.assertEqual(16.45, v.m90 * 1)
        self.assertEqual(19.6, v.m95 * 1)
        self.assertEqual(25.75, v.m99 * 1)

        # Convert to margins and back to se
        self.assertAlmostEqual(10.0, v.m90.se)
        self.assertAlmostEqual(10.0, v.m95.se)
        self.assertAlmostEqual(10.0, v.m95.se)

        print(vt.RateVT(0))
        print(vt.RateVT(None))

