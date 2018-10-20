# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" Code generation for processing columns

"""

import ast


def file_loc():
    """Return file and line number"""
    import sys
    import inspect
    try:
        raise Exception
    except:
        file_ = '.../' + '/'.join((inspect.currentframe().f_code.co_filename.split('/'))[-3:])
        line_ = sys.exc_info()[2].tb_frame.f_back.f_lineno
        return "{}:{}".format(file_, line_)


const_args = ('row', 'row_n', 'scratch', 'errors', 'accumulator', 'pipe', 'manager', 'source')
var_args = ('v', 'i_s', 'i_d', 'header_s', 'header_d')
all_args = var_args + const_args

# Full lambda definition for a column, including variable parts
col_code_def = 'lambda {}:'.format(','.join(all_args))

# lambda definition for the who;e row. Includes only the arguments
# that are the same for every column
code_def = 'lambda {}:'.format(','.join(const_args))

col_args_t = """col_args = dict(v=v, i_s=i_s, i_d=i_d, header_s=header_s, header_d=header_d,
              scratch=scratch, errors=errors, accumulator = accumulator,
              row=row, row_n=row_n)"""

file_header = """
# {}
import sys
from rowgenerators.valuetype import resolve_value_type
from rowgenerators.rowpipe.exceptions import CasterExceptionError
""".format(file_loc())

column_template = """
# {loc}
def {f_name}(v, i_s, i_d, header_s, header_d, row, row_n, errors, scratch, accumulator, pipe, manager, source):

    try:
{stack}

    except Exception as exc:

{exception}

    return v
"""

indent = '        '

row_template = """
# {loc}
def row_{table}_{stage}(row, row_n, errors, scratch, accumulator, pipe, manager, source):

    return [
{stack}
    ]
"""




class CodeGenError(Exception):
    pass


def exec_context(**kwargs):
    """Base environment for evals, the stuff that is the same for all evals. Primarily used in the
    Caster pipe"""
    import dateutil.parser
    import datetime
    import random
    from functools import partial
    from rowgenerators.valuetype.types import parse_date, parse_time, parse_datetime
    import rowgenerators.valuetype.types
    import rowgenerators.valuetype.exceptions
    import rowgenerators.valuetype.test
    import rowgenerators.valuetype
    import rowgenerators.rowpipe.transforms

    def set_from(f, frm): # This maybe isn't used anymore, or maybe it is for debugging?
        try:
            try:
                f.ambry_from = frm
            except AttributeError:  # for instance methods
                f.im_func.ambry_from = frm
        except (TypeError, AttributeError):  # Builtins, non python code
            pass

        return f

    test_env = dict(
        parse_date=parse_date,
        parse_time=parse_time,
        parse_datetime=parse_datetime,
        partial=partial
    )

    test_env.update(kwargs)
    test_env.update(dateutil.parser.__dict__)
    test_env.update(datetime.__dict__)
    test_env.update(random.__dict__)
    test_env.update(rowgenerators.valuetype.core.__dict__)
    test_env.update(rowgenerators.valuetype.types.__dict__)
    test_env.update(rowgenerators.valuetype.exceptions.__dict__)
    test_env.update(rowgenerators.valuetype.test.__dict__)
    test_env.update(rowgenerators.valuetype.__dict__)
    test_env.update(rowgenerators.rowpipe.transforms.__dict__)

    localvars = {}

    for f_name, func in test_env.items():
        if not isinstance(func, (str, tuple)):
            localvars[f_name] = set_from(func, 'env')

    # The 'b' parameter of randint is assumed to be a manager, but
    # replacing it with a lambda prevents the param assignment
    localvars['randint'] = lambda a, b: random.randint(a, b)

    localvars['round'] = lambda a, b: round(a, b)

    return localvars


def make_row_processors(source_headers, dest_table, env=None):
    """
    Make multiple row processors for all of the columns in a table.

    :param source_headers:
    :param dest_table:
    :param env:

    :return:
    """

    import re

    if env is None:
        env = exec_context()

    assert len(dest_table.columns) > 0

    # Convert the transforms to a list of list, with each list being a
    # segment of column transformations, and each segment having one entry per column.

    row_processors = []

    out = []
    preamble = []

    transforms = dest_table.stage_transforms

    for i, segments in enumerate(transforms):  # Iterate over each stage

        column_names = []
        column_types = []
        seg_funcs = []

        # Iterate over each column, linking it to the segments for this stage
        for col_num, (segment, column) in enumerate(zip(segments, dest_table), 0):

            assert column
            assert column.name, (dest_table.name, i)
            assert column.name == segment['column'].name

 
            col_name = column.name
            preamble_parts, try_lines, exception, passthrough = make_stack(env, i, segment)

            preamble += preamble_parts

            column_names.append(col_name)
            column_types.append(column.datatype)

            # Optimization to remove unecessary functions. Without this, the column function will
            # have just 'v = v'
            if len(segment['transforms']) == 1 and segment['transforms'][0] == 'v':
                seg_funcs.append('row[{}]'.format(col_num))
                continue

            column_name = re.sub(r'[^\w]+', '_', col_name, )
            table_name = re.sub(r'[^\w]+', '_', dest_table.name)

            assert column_name, (dest_table.name, i, col_name)
            assert table_name

            f_name = "{}_{}_{}".format(table_name, column_name, i)

            exception = (exception if exception
                         else 'raise CasterExceptionError("' + f_name + '",header_d, v, exc, sys.exc_info())')

            try:
                # The input values for the first stage is the input dataset,
                # which may have different columns that the later stages
                if i == 0:
                    i_s = source_headers.index(column.name)
                    header_s = column.name

                else:
                    i_s = col_num
                    header_s = None

                v = 'row[{}]'.format(i_s)

            except ValueError as e:  # The col name is not in the source dataset
                # This is the typical case when the output dataset has different columns from the
                # input, whcih should only occur on the first stage.
                i_s = 'None'
                header_s = None
                v = 'None' if col_num >= 1 else 'row_n'  # Give the id column (first column) the row number

            header_d = column.name

            # Seg funcs holds the calls to the function for each column, called in the row stage function
            seg_funcs.append(f_name
                             + ('({v}, {i_s}, {i_d}, {header_s}, \'{header_d}\', '
                                'row, row_n, errors, scratch, accumulator, pipe, manager, source)')
                             .format(v=v,
                                     i_s=i_s,
                                     i_d=col_num,
                                     header_s="'" + header_s + "'" if header_s else 'None',
                                     header_d=header_d))

            # This creates the column manipulation function.
            out.append(column_template.format(
                f_name=f_name,
                table_name=dest_table.name,
                column_name=col_name,
                stage=i,
                i_s=i_s,
                i_d=col_num,
                header_s=header_s,
                header_d=header_d,
                v=v,
                exception=indent + exception,
                stack='\n'.join(indent + l for l in try_lines),
                col_args='',  # col_args not implemented yet
                loc=file_loc()))

        # This stack assembles all of the function calls that will generate the next row
        stack = '\n'.join("{}{}, # column {}".format(indent, l, cn)
                          for l, cn, dt in zip(seg_funcs, column_names, column_types))

        out.append(row_template.format(
            table=re.sub(r'[^\w]+', '_', dest_table.name),
            stage=i,
            stack=stack,
            loc=file_loc()
        ))

        row_processors.append('row_{table}_{stage}'.format(stage=i,
                                                           table=re.sub(r'[^\w]+', '_', dest_table.name)))

    # Add the final datatype cast, which is done seperately to avoid an unecessary function call.
    stack = '\n'.join("{}cast_{}(row[{}], '{}', errors),".format(indent, c.datatype.__name__, i, c.name)
                      for i, c in enumerate(dest_table))

    out.append(row_template.format(
        table=re.sub(r'[^\w]+', '_', dest_table.name),
        stage=len(transforms),
        stack=stack,
        loc=file_loc()
    ))

    row_processors.append('row_{table}_{stage}'.format(stage=len(transforms),
                                                       table=re.sub(r'[^\w]+', '_', dest_table.name)))

    out.append('row_processors = [{}]'.format(','.join(row_processors)))

    return '\n'.join([file_header] + list(set(preamble)) + out)


def calling_code(f, f_name=None, raise_for_missing=True):
    """Return the code string for calling a function. """
    import inspect
    from rowgenerators.exceptions import ConfigurationError

    if inspect.isclass(f):
        try:
            args = inspect.signature(f.__init__).parameters.keys()
        except TypeError as e:
            raise TypeError("Failed to inspect {}: {}".format(f, e))

    else:
        args = inspect.signature(f).parameters.keys()

    if len(args) > 1 and list(args)[0] == 'self':
        args = list(args)[1:]

    if 'self' in args:  # Python3 gets self, but not Python2
        args.remove('self')

    for a in args:
        if a not in all_args + ('exception',):  # exception arg is only for exception handlers
            if raise_for_missing:
                # In CPython, inspecting __init__ for IntMeasure, FloatMeasure, etc,
                # raises a TypeError 12 lines up, but that does not happen in PyPy. This hack
                # raises the TypeError.
                if a == 'obj':
                    raise TypeError()

                raise TypeError()
                raise ConfigurationError('Caster code {} has unknown argument '
                                         'name: \'{}\'. Must be one of: {} '.format(f, a, ','.join(all_args)))

    arg_map = {e: e for e in var_args}

    args = [arg_map.get(a, a) for a in args]

    return "{}({})".format(f_name if f_name else f.__name__, ','.join(args))

def make_stack(env, stage, segment):
    """For each transform segment, create the code in the try/except block with the
    assignements for pipes in the segment """

    import string
    import random
    from rowgenerators.valuetype import ValueType

    passthrough = False  # If true, signal that the stack will just return its input value

    column = segment['column']

    def make_line(column, t):
        preamble = []

        line_t = "v = {} # {}"

        env_t = env.get(t,t)

        if isinstance(env_t, type) and issubclass(env_t, ValueType):  # A valuetype class, from the datatype column.

            try:
                cc, fl = calling_code(env_t, env_t.__name__), file_loc()
            except TypeError as e:
                cc, fl = "{}(v)".format(env_t.__name__), file_loc()

            preamble.append("{} = resolve_value_type('{}') # {}".format(env_t.__name__, env_t.vt_code, file_loc()))

        elif isinstance(t, type):  # A python type, from the datatype columns.
            cc, fl = "parse_{}(v, header_d)".format(t.__name__), file_loc()

        elif callable(env.get(t)):  # Transform function
            try:
                cc, fl = calling_code(env.get(t), t), file_loc()
            except TypeError as e:
                raise

        else:  # A transform generator, or python code.

            rnd = (''.join(random.choice(string.ascii_lowercase) for _ in range(6)))

            name = 'tg_{}_{}_{}'.format(column.name, stage, rnd)
            try:
                a, b, fl = rewrite_tg(env, name, t)
            except (CodeGenError, AttributeError) as e:
                raise CodeGenError("Failed to re-write pipe code '{}' in column '{}': {} "
                                   .format(t, column, e))

            cc = str(a)

            if b:
                preamble.append("{} = {} # {}".format(name, b, file_loc()))

        line = line_t.format(cc, fl)

        return line, preamble

    preamble = []

    try_lines = []


    for t in list(segment):

        if not t:
            continue

        line, col_preamble = make_line(column, t)

        preamble += col_preamble
        try_lines.append(line)

    if segment['exception']:
        exception, col_preamble = make_line(column, segment['exception'])
    else:
        exception = None

    if len(try_lines) == 0:
        try_lines.append('pass # Empty pipe segment')

    assert len(try_lines) > 0, column.name

    return preamble, try_lines, exception, passthrough


def mk_kwd_args(fn, fn_name=None):
    import inspect

    fn_name = fn_name or fn.__name__

    fn_args = inspect.getargspec(fn).args

    if len(fn_args) > 1 and fn_args[0] == 'self':
        args = fn_args[1:]

    kwargs = dict((a, a) for a in all_args if a in args)

    return "{}({})".format(fn_name, ','.join(a + '=' + v for a, v in kwargs.items()))


class ReplaceTG(ast.NodeTransformer):
    """Replace a transform generator with the transform function"""

    def __init__(self, env, tg_name):
        super(ReplaceTG, self).__init__()

        self.tg_name = tg_name
        self.trans_gen = None
        self.env = env
        self.loc = ''

    def missing_args(self):
        pass

    def visit_Call(self, node):  # pragma: no cover

        import inspect
        from rowgenerators.valuetype import is_transform_generator
        import types

        if not isinstance(node.func, ast.Name):
            self.generic_visit(node)
            # Python 3.5 removed starargs, which the meta module, which dumps the AST to code,
            # still expects. Not sure about the need for kwargs, but meta expects that too
            node.starargs = None
            node.kwargs = None
            return node

        fn_name = node.func.id
        fn_args = None
        use_kw_args = True

        fn = self.env.get(node.func.id)
        self.loc = file_loc()  # Not a builtin, not a type, not a transform generator

        # In this case, the code line is a type that has a parse function, so rename it.
        if not fn:
            t_fn_name = 'parse_' + fn_name
            t_fn = self.env.get(t_fn_name)
            if t_fn:
                self.loc = file_loc()  # The function is a type
                fn, fn_name = t_fn, t_fn_name

        # Ok, maybe it is a builtin
        if not fn:
            o = eval(fn_name)
            if isinstance(o, types.BuiltinFunctionType):
                self.loc = file_loc()  # The function is a builtin
                fn = o
                fn_args = ['v']
                use_kw_args = False

        if not fn:
            raise CodeGenError("Failed to get function named '{}' from the environment".format(node.func.id))

        if not fn_args:
            fn_args = inspect.getargspec(fn).args

        # Create a dict of the arguments that have been specified
        used_args = dict(tuple(zip(fn_args, node.args))
                         + tuple((kw.arg, kw.value) for kw in node.keywords)
                         )

        # Add in the arguments that were not, but only for args that are specified to be
        # part of the local environment
        for arg in fn_args:
            if arg not in used_args and arg in all_args:
                used_args[arg] = ast.Name(id=arg, ctx=ast.Load())

        # Now, all of the args are in a dict, so we'll re-build them as
        # as if they were all kwargs. Any arguments that were not provided by the
        # signature in the input are added as keywords, with the value being
        # a variable of the same name as the argument: ie. if 'manager' was defined
        # but not provided, the signature has an added 'manager=manager' kwarg

        keywords = [ast.keyword(arg=k, value=v) for k, v in used_args.items()]

        tg_ast = ast.copy_location(
            ast.Call(
                func=ast.Name(id=fn_name, ctx=ast.Load()),
                args=[e.value for e in keywords] if not use_kw_args else [],  # For builtins, which only take one arg
                keywords=keywords if use_kw_args else [],
                starargs=[],
                kwargs=[]
            ), node)

        if is_transform_generator(fn):
            raise Exception("Deprecated?")
            self.loc = file_loc()  # The function is a transform generator.
            self.trans_gen = tg_ast
            replace_node = ast.copy_location(
                ast.Call(
                    func=ast.Name(id=self.tg_name, ctx=ast.Load()),
                    args=[],
                    keywords=[],
                    kwargs=ast.Name(id='col_args', ctx=ast.Load()),
                    starargs=[]
                ), node)

        else:

            replace_node = tg_ast


        return replace_node


def rewrite_tg(env, tg_name, code):
    """Re-write a transform generating function pipe specification by extracting the transform generating part,
    and replacing it with the generated transform. so:

       tgen(a,b,c).foo.bar

    becomes:

        tg = tgen(a,b,c)

        tg.foo.bar

    """

    import meta  # Byte-code and ast programming tools
    import codegen

    visitor = ReplaceTG(env, tg_name)
    assert visitor.tg_name

    try:
        tree = visitor.visit(ast.parse(code.strip()))
    except SyntaxError as e:
        raise SyntaxError(str(e) + "\nIn code: \n" + code)

    if visitor.loc:
        loc = ' #' + visitor.loc
    else:
        loc = file_loc()  # The AST visitor didn't match a call node

    if visitor.trans_gen:
        tg = meta.dump_python_source(visitor.trans_gen).strip()
    else:
        tg = None


    return meta.dump_python_source(tree).strip(), tg, loc
