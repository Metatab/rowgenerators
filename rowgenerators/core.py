

import inspect
import collections
from pkg_resources import load_entry_point, iter_entry_points

def get_generator(source):

    names = []

    if inspect.isgenerator(source):
        names.append('<generator>')

    if isinstance(source, collections.Iterable):
        names.append('<iterator>')

    try:
        names.append('.{}'.format(source.target_format))
    except AttributeError:
        pass

    classes = sorted([ep.load() for ep in iter_entry_points(group='rowgenerators') if ep.name in names],
                     key=lambda cls: cls.priority)

    return classes



