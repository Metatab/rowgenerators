def get_file(url_str):

    from rowgenerators import  parse_app_url

    u = parse_app_url(url_str)

    return u.get_resource().get_target()


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