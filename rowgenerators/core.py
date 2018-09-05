



def get_generator(source, **kwargs):

    import inspect
    import collections
    from pkg_resources import iter_entry_points
    from rowgenerators.exceptions import RowGeneratorError
    from rowgenerators.appurl import parse_app_url, Url
    from rowgenerators.source import Source

    names = []

    if isinstance(source, Source):
        return source

    if isinstance(source, str):

        ref = parse_app_url(source).get_resource().get_target()
        try:
            names.append('.{}'.format(ref.target_format))
        except AttributeError:
            pass

    elif inspect.isgenerator(source):
        names.append('<generator>')
        ref = source

    elif isinstance(source, collections.Iterable):
        names.append('<iterator>')
        ref = source

    elif hasattr(source, '__iter__'):
        names.append('<iterator>')
        ref = source

    elif isinstance(source, Url):
        ref = source
        try:
            names.append('.{}'.format(ref.target_format))
        except AttributeError:
            pass

        try:
            names.append('{}+'.format(ref.scheme_extension))
        except AttributeError:
            pass

        try:
            names.append('{}:'.format(ref.scheme))
        except AttributeError:
            pass

        try:
            names.append('<{}>'.format(ref.__class__.__name__))
        except AttributeError:
            pass

        try:
            if ref.generator_class:
                names.append('<{}>'.format(ref.generator_class.__name__))
        except AttributeError:
            pass



    else:
        raise RowGeneratorError("Unknown arg type for source {}, type='{}'".format(source, type(source)))

    classes = sorted([ep.load() for ep in iter_entry_points(group='rowgenerators') if ep.name in names],
                     key=lambda cls: cls.priority)


    if not classes:
        raise RowGeneratorError(("Can't find generator for source '{}' \nproto={}, "
                                  "resource_format={}, target_format={}, names={} ")
                                 .format(source, ref.proto, ref.resource_format, ref.target_format, names))
    try:
        return classes[0](ref, **kwargs)
    except NotImplementedError:
        raise
    except Exception as e:
        raise RowGeneratorError("Failed to instantiate generator for class '{}', ref '{}'".format(classes[0], ref)) from e

class SelectiveRowGenerator(object):
    """Proxies an iterator to remove headers, comments, blank lines from the row stream.
    The header will be emitted first, and comments are avilable from properties """

    def __init__(self, seq, start=0, headers=[], comments=[], end=[], load_headers=True, **kwargs):
        """
        An iteratable wrapper that coalesces headers and skips comments

        :param seq: An iterable
        :param start: The start of data row
        :param headers: An array of row numbers that should be coalesced into the header line, which is yieled first
        :param comments: An array of comment row numbers
        :param end: The last row number for data
        :param kwargs: Ignored. Sucks up extra parameters.
        :return:
        """

        self.iter = iter(seq)
        self.start = start if (start or start is 0) else 1
        self.header_lines = headers if isinstance(headers, (tuple, list)) else [int(e) for e in headers.split(',') if e]
        self.comment_lines = comments
        self.end = end

        self.load_headers = load_headers

        self.headers = []
        self.comments = []

        int(self.start)  # Throw error if it is not an int

    @property
    def coalesce_headers(self):
        """Collects headers that are spread across multiple lines into a single row"""

        import re

        if not self.headers:
            return None

        header_lines = [list(hl) for hl in self.headers if bool(hl)]

        if len(header_lines) == 0:
            return []

        if len(header_lines) == 1:
            return header_lines[0]

        # If there are gaps in the values of a line, copy them forward, so there
        # is some value in every position
        for hl in header_lines:
            last = None
            for i in range(len(hl)):
                hli = str(hl[i])
                if not hli.strip():
                    hl[i] = last
                else:
                    last = hli

        headers = [' '.join(str(col_val).strip() if col_val else '' for col_val in col_set)
                   for col_set in zip(*header_lines)]

        headers = [re.sub(r'\s+', ' ', h.strip()) for h in headers]

        return headers

    def __iter__(self):

        for i, row in enumerate(self.iter):

            if i in self.header_lines:
                if self.load_headers:
                    self.headers.append(row)
            elif i in self.comment_lines:
                self.comments.append(row)
            elif i == self.start:
                break

        if self.headers:

            headers = self.coalesce_headers
        else:
            headers = ['col' + str(i) for i, _ in enumerate(row)]

        yield headers

        yield row

        yield from self.iter
