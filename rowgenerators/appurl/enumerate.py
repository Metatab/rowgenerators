# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""Download resoruces and generate collections of URLs with targets and segments for
the files containes in Excel and ZIP files"""



def inspect(url, callback=None):
    """Return a list of possible extensions to the url, such as files within a ZIP archive, or
    worksheets in a spreadsheet"""
    from zipfile import ZipFile
    from copy import deepcopy

    raise NotImplementedError()

    if callback:
        callback("Inspecting: format={} file={} segment={} url={}".format(
            url.target_format, url.target_file, url.target_segment, url.rebuild_url()))

    if url.is_archive and url.target_file is None:

        if url.target_file is None:
            l = []

            for file_name in real_files_in_zf(zf):
                l.append(ss.update(target_file=file_name))

            return l

        elif ss.target_format in ('xls', 'xlsx') and ss.target_segment is None:
            src = get_generator(ss, cache_fs)
            l = []
            for seg in src.children:
                ss2 = ss.update(target_segment=seg)
                l.append(ss2)

            return l

    if ss.target_format in ('xls', 'xlsx') and ss.target_segment is None:

        src = get_generator(ss, cache_fs)

        l = []
        for seg in src.children:
            ss2 = ss.update(target_segment=seg)
            l.append(ss2)

        return l

    return [deepcopy(ss)]

def enumerate_contents(base_spec, cache_fs, callback=None):
    """Inspect the URL, and if it is a container ( ZIP Or Excel ) inspect each of the contained
    files. Yields all of the lower-level URLs"""

    from rowgenerators.appurl.url import Url, parse_app_url

    if not isinstance(base_spec, Url):
        base_spec = parse_app_url(url=base_spec)

    for s in inspect(base_spec, cache_fs, callback=callback):
        for s2 in inspect(s, cache_fs, callback=callback):
            yield s2

