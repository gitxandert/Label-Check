"""
This script looks for hardcoded variables and CLI arguments (as parsed through 
the argparse module) inside of other Python scripts. It will be used with 
runner.py to find arguments that could be configured into a syntax class Args. 
This syntax class will be used to store and feed arguments into these scripts 
in a sequence facilitated by runner.py. All global and hardcoded variables will 
be given equivalents in argparse and be set to the values culled from argparse 
at runtime. All argparse arguments will be stored in a syntax class Args that 
will be created at the top of each script, to be read by runner.py --config 
later. When runner.py --config is run, it allows the user to create and edit 
class Args, as well as create and edit runtime values to be fed into these 
arguments at runtime.

Since this script (and runner.py) make edits to the targeted scripts, it is
recommended to make copies of the scripts intended to be stitched together.
"""

import os, sys
import ast
from pathlib import Path
from typing import Optional, Any
from runner import ArgSpec, ScriptArgs


class ParseArgsError(Exception):
    def __init__(self, message: str, path: Path, lineno: int = None, offset: int = None):
        errstring = path.name
        if lineno is not None:
            errstring += f":{lineno}"
            if offset is not None:
                errstring += f":{offset}"
        errstring += f": {message}"
        super().__init__(errstring)


def parse_assign(path: Path, stmt: ast.Assign) -> list[ArgSpec]:
    names = []
    if isinstance(stmt.targets[0], ast.Name):
        names.append(stmt.targets[0].id)
    elif isinstance(stmt.targets[0], ast.Tuple):
        elts = stmt.targets[0].elts
        for n in elts:
            if isinstance(n, ast.Name):
                names.append(n.id)
            else:
                raise ParseArgsError(
                        "Unrecognized lvalue",
                        path=path,
                        lineno=stmt.lineno,
                        offset=stmt.col_offset
                    )
    else:
        print(ast.dump(stmt.targets[0], indent=4))
        raise ParseArgsError(
                    "Unrecognized lvalue",
                    path=path,
                    lineno=stmt.lineno,
                    offset=stmt.col_offset
                )

    form = None
    st_val = stmt.value
    if isinstance(st_val, ast.Constant):
        value = st_val.value
        if isinstance(value, bool):
            kind = "bool"
        elif isinstance(value, int):
            kind = "int"
        elif isinstance(value, float):
            kind = "float"
        elif isinstance(value, str):
            if '/' in value or '\\' in value:
                kind = "path"
            else:
                kind = "str"
        else:
            raise ParseArgsError(
                    f"Couldn't identify type of {value}",
                    path=path,
                    lineno=st_val.lineno,
                    offset=st_val.col_offset,
                )
    elif isinstance(st_val, ast.Tuple):
        vals = []
        form = "("
        end = st_val.elts[len(st_val.elts) - 1]
        for v in st_val.elts:
            if isinstance(v, ast.Constant):
                vals.append(v.value)
                if v == end:
                    form += f"{type(v.value).__name__})"
                else:
                    form += f"{type(v.value).__name__}, "
            else:
                raise ParseArgsError(
                        "All rvalues must be constant",
                        path=path,
                        lineno=st_val.lineno,
                        offset=st_val.col_offset,
                    )
        value = tuple(vals)
        kind = "tuple"
    elif isinstance(st_val, ast.List):
        value = []
        form = type(st_val.elts[0].value)
        for v in st_val.elts:
            if isinstance(v, ast.Constant):
                if type(v.value) == form:
                    value.append(v.value)
                else:
                    raise ParseArgsError(
                            "list values must be of same type",
                            path=path,
                            lineno=st_val.lineno,
                            offset=st_val.col_offset,
                        )
            else:
                raise ParseArgsError(
                        "All rvalues must be constant",
                        path=path,
                        lineno=st_val.lineno,
                        offset=st_val.col_offset,
                    )
        kind = "list"
        form = f"[{form.__name__}]"
    else:
        raise ParseArgsError(
                "Unrecognized rvalue",
                path=path,
                lineno=st_val.lineno,
                offset=st_val.col_offset
            )

    specs = []
    for name in names:
        sp = ArgSpec(
                name=name,
                kind=kind,
                required=True, # default True, since hardcoded
                form=form,
                default=value,
                desc=None,
            )
        specs.append(sp)

    return specs


def match_nargs(path: Path, stmt: ast.Call, nargs: int | str, kind: str | None, default: Any) -> (str, str, bool):
    if default is None and kind is None:
        raise ParseArgsError(
                "nargs requires either type or default to extrapolate form",
                path=path,
                lineno=stmt.lineno,
                offset=stmt.col_offset,
            )

    form = None
    required = False
    # if nargs is int, then it is a tuple
    if isinstance(nargs, int):
        form = '('
        if kind is not None:
            # the tuple admits a single type;
            # possibly most tuples fed to argparse will be like this
            if nargs > 1:
                for _ in range(nargs - 1):
                    form += f"{kind}, "
            form += f"{kind})"
        else:
            # the tuple's form is defined by the default argument
            if isinstance(default, list):
                end = default[len(default) - 1]
                for d in default:
                    if d != end:
                        form += f"{type(d).__name__}, "
                form += f"{type(end).__name__})"
            else:
                form += f"{type(default).__name__})"
        kind = 'tuple'
    else:
        match nargs:
            case '+':
                # one or more
                required = True
                if default is not None:
                    if isinstance(default, list):
                        # if no kind was provided, then derive it
                        # from the first member of the default list
                        if kind is None:
                            kind = type(default[0]).__name__
                        for d in default:
                            if type(d).__name__ != kind:
                                raise ParseArgsError(
                                    "list variables must all be the same type",
                                    path=path,
                                    lineno=stmt.lineno,
                                    offset=stmt.col_offset,
                                )
                    else:
                        # can accept a mono default,
                        # since nargs suggests a possible list
                        if kind is None:
                            kind = type(default).__name__
                        else:
                            if type(default).__name__ != kind:
                                raise ParseArgsError(
                                        "type and default do not match",
                                        path=path,
                                        lineno=stmt.lineno,
                                        offset=stmt.col_offset,
                                    )
                    required = False
                form = f'[{kind}]'
                kind = 'list'
            case '*':
                # possibly 0; not required
                if default is not None:
                    if isinstance(default, list):
                        # if no kind was provided, then derive it
                        # from the first member of the default list
                        if kind is None:
                            kind = type(default[0]).__name__
                        for d in default:
                            if type(d).__name__ != kind:
                                raise ParseArgsError(
                                    "list variables must all be the same type",
                                    path=path,
                                    lineno=stmt.lineno,
                                    offset=stmt.col_offset,
                                )
                    else:
                        # can accept a mono default,
                        # since nargs suggests a possible list
                        if kind is None:
                            kind = type(default).__name__
                        else:
                            if type(default).__name__ != kind:
                                raise ParseArgsError(
                                        "type and default do not match",
                                        path=path,
                                        lineno=stmt.lineno,
                                        offset=stmt.col_offset,
                                    )                
                form = f'[{kind}]'
                kind = 'list'
                required = False
            case '?':
                # 0 or 1
                if default is not None:
                    if not isinstance(default, list):
                        if kind is None:
                            kind = type(default).__name__
                        else:
                            if type(default).__name__ != kind:
                                raise ParseArgsError(
                                        "type and default do not match",
                                        path=path,
                                        lineno=stmt.lineno,
                                        offset=stmt.col_offset,
                                    )
                    else:
                        raise ParseArgsError(
                                "A nargs argument of '?' cannot have a default argument that is a list",
                                path=path,
                                lineno=stmt.lineno,
                                offset=stmt.col_offset,
                            )
            case _:
                raise ParseArgsError(
                        f"Invalid argument for nargs '{nargs}'",
                        path=path,
                        lineno=stmt.lineno,
                        offset=stmt.col_offset,
                    )

    return kind, form, required


def parse_argparse_arg(path: Path, stmt: ast.Call) -> ArgSpec:
    # just use the first name
    if isinstance(stmt.args[0], ast.Constant):
        name = stmt.args[0].value
    else:
        raise ParseArgsError(
                "Variable names must be constants",
                path=path,
                lineno=stmt.args.lineno,
                offset=stmt.args.col_offset,
            )

    kind = None
    required = False
    form = None
    default = None
    desc = None
    # store_const means that a default const variable is
    # stored via argparse.add_argument()
    # (some alt to 'default' that I don't get the point of)
    store_const = False
    const = None
    # nargs can be set to a constant or special char;
    # if it is not none, the argument to 'kind' becomes the
    # argument to 'form', and 'kind' becomes either 'tuple'
    # or 'list' (depending on whether nargs is a constant)
    nargs = None

    for kw in stmt.keywords:
        match kw.arg:
            case 'type':
                if isinstance(kw.value, ast.Name):
                    kind = kw.value.id
                else:
                    raise ParseArgsError(
                            f"Invalid type '{kw.value.id}'",
                            path=path,
                            lineno=kw.lineno,
                            offset=kw.col_offset,
                        )
            case 'required':
                if kw.value.value == True \
                        or kw.value.value == False:
                    required = kw.value.value
                else:
                    raise ParseArgsError(
                            f"required argument must be either 'True' or 'False'",
                            path=path,
                            lineno=kw.lineno,
                            offset=kw.col_offset,
                        )
            case 'help':
                # don't even attempt formatted strings
                if not isinstance(kw.value, ast.JoinedStr):
                    if isinstance(kw.value, ast.Constant):
                        desc = kw.value.value
                    else:
                        raise ParseArgsError(
                                "help argument must be a str constant",
                                path=path,
                                lineno=kw.lineno,
                                offset=kw.col_offset,
                            )
            case 'action':
                if not isinstance(kw.value, ast.Constant):
                    raise ParseArgsError(
                            "action argument requires a constant value",
                            path=path,
                            lineno=kw.lineno,
                            offset=kw.col_offset,
                        )
                else:
                    match kw.value.value:
                        case 'store_true':
                            kind = 'bool'
                            default = 'True'
                        case 'store_false':
                            kind = 'bool'
                            default = 'False'
                        case 'store':
                            # default behavior
                            pass
                        case 'store_const':
                            # should be accompanied with
                            # const argument
                            store_const = True
                        case _:
                            # don't bother parsing
                            raise ParseArgsError(
                                    "Possibly complicated or incorrectly formed argument",
                                    path=path,
                                    lineno=kw.lineno,
                                    offset=kw.col_offset,
                                )
            case 'nargs':
                # nargs implies either a list or tuple
                if isinstance(kw.value, ast.Constant):
                    nargs = kw.value.value
                else:
                    raise ParseArgsError(
                            "nargs argument must be a constant value",
                            path=path,
                            lineno=kw.lineno,
                            offset=kw.col_offset,
                        )
            case 'const':
                if isinstance(kw.value, ast.Constant):
                    const = kw.value.value
                else:
                    raise ParseArgsError(
                            "const argument must be a constant value",
                            path=path,
                            lineno=kw.lineno,
                            offset=kw.col_offset,
                        )
            case 'default':
                if isinstance(kw.value, ast.Constant):
                    default = kw.value.value
                elif isinstance(kw.value, ast.List):
                    elts = kw.value.elts
                    default = []
                    for v in elts:
                        if isinstance(v, ast.Constant):
                            default.append(v.value)
                        else:
                            raise ParseArgsError(
                                    "list variables must be constants",
                                    path=path,
                                    lineno=v.lineno,
                                    offset=v.col_offset,
                                )
                else:
                    raise ParseArgsError(
                            "default arguments must be constants or lists",
                            path=path,
                            lineno=kw.value.lineno,
                            offset=kw.value.col_offset,
                        )
            case 'choices' | 'dest' | 'deprecated':
                # ignore
                pass
            case _:
                raise ParseArgsError(
                        f"Unknown argument '{kw.arg}'",
                        path=path,
                        lineno=kw.lineno,
                        offset=kw.col_offset,
                    )

    # override default with const
    # (seems silly, but here we are)
    if store_const:
        if const is not None:
            default = const
        else:
            raise ParseArgsError(
                    "store_const specified without accompanying const",
                    path=path,
                    lineno=stmt.lineno,
                    offset=stmt.col_offset,
                )

    # test nargs before testing kinds
    # to assign kind if possible
    if nargs is not None:
        kind, form, required = match_nargs(path, stmt, nargs, kind, default)
   
    # ensure type argument was provided;
    # if not, sus out type from default and nargs
    if kind is None:
        if default is not None:
            if nargs is None:
                # is neither list nor tuple
                if not isinstance(default, list):
                    kind = type(default).__name__
                else:
                    # don't assume tuple(?)
                    raise ParseArgsError(
                            "default argument can only be a list with nargs specified",
                            path=path,
                            lineno=stmt.lineno,
                            offset=stmt.col_offset,
                        )
            else:
                raise ParseArgsError(
                        "type cannot be derived",
                        path=path,
                        lineno=stmt.lineno,
                        offset=stmt.col_offset,
                    )
        else:
            raise ParseArgsError(
                    "type or default must be provided",
                    path=path,
                    lineno=stmt.lineno,
                    offset=stmt.col_offset,
                )

    return ArgSpec(
            name=name,
            kind=kind,
            required=required,
            form=form,
            default=default,
            desc=desc,
        )


def print_args(spec: ArgSpec):
    print(f"{spec.name}")
    if spec.desc is not None:
        print(f"\tdesc={spec.desc}")
    print(f"\tkind={spec.kind}")
    print(f"\trequired={spec.required}")
    if spec.form is not None:
        print(f"\tform={spec.form}")
    if spec.default is not None:
        print(f"\tdefault={spec.default}")


def edit_script(
        script_path: Path,
        ap_args: list[ArgSpec], 
        g_var: list[ArgSpec], 
        m_var: list[ArgSpec], 
        parser: str | None
    ):
    with open(script_path, 'r') as f:
        cur = f.read()
    
    new = ''
    if parser is not None:
        print(f"Adding arguments to {parser}")
    else:
        print(f"Importing argparse")
        new = "import argparse\n\n"

    cur = cur.split('\n')
    for sp in ap_args:
        print_args(sp)
    for sp in g_var:
        print_args(sp)
    for sp in m_var:
        print_args(sp)

    new += cur
    with open(script_path, 'w') as f:
        f.write(new)


if __name__ == "__main__":
    scripts = {}
    for script in Path("dummies").iterdir():
        # only parse Python scripts (might extend to .sh later)
        if script.suffix != ".py":
            continue

        tree = ast.parse(script.read_text())

        # collect three separate groups of args/variables:
        # - global variables
        # - hardcoded variables in main
        # - arguments parsed by argparse
        # Arguments parsed by argparse will be automatically
        # included in class Args. Optional arguments that user approves
        # will first be added to main via argparse.add_argument(),
        # then annotated in class Args.
        args = {}
        g_var = []
        m_var = []
        ap_args = []

        # save parser name for later;
        # can also check against to see if should import argparse
        parser = None

        for stmt in tree.body:
            # look for global variables
            # (ignore variables set to another variable name)
            if isinstance(stmt, ast.Assign) \
            and not isinstance(stmt.value, ast.Name):
                g_var.append(stmt)
            # look for 'if __name__ == "__main__":'
            if isinstance(stmt, ast.If):
                test = stmt.test
                if test.left.id == "__name__" \
                and isinstance(test.ops[0], ast.Eq) \
                and test.comparators[0].value == "__main__":
                    for if_stmt in stmt.body:
                        val = if_stmt.value
                        # look for hardcoded variables
                        if isinstance(if_stmt, ast.Assign):
                            if not isinstance(val, ast.Call) \
                            and not isinstance(val.value, ast.Name):
                                m_var.append(if_stmt)
                        # look for argparse args
                        elif isinstance(if_stmt, ast.Expr):
                            if isinstance(val, ast.Call):
                                func = val.func
                                if isinstance(func, ast.Attribute):
                                    if func.attr == 'add_argument':
                                        ap_args.append(val)
                                        if parser is None:
                                            parser = val.func.value.id
        args["g_var"] = g_var
        args["m_var"] = m_var
        args["ap_args"] = ap_args
        args["parser"] = parser
        scripts[script] = args

    for s, args in scripts.items():
        print(f"\n----{s}----")
        auto = len(args['ap_args'])
        g_opt = len(args['g_var'])
        m_opt = len(args['m_var'])
        if auto > 0:
            print(f"{auto} arg(s) will be added to class Args")
        if g_opt > 0:
            print(f"{g_opt} global variable(s) can be made into arguments")
            convert_g = input("Create arguments for global variables? [y/N] ")
            convert_g = convert_g.strip().lower()
        if m_opt > 0:
            print(f"{m_opt} variable(s) in main can be made into arguments")
            convert_m = input("Create arguments for variables in main? [y/N] ")
            convert_m = convert_m.strip().lower()
        if auto + g_opt + m_opt == 0:
            print("No arguments, global variables, or hardcoded variables in main were found.")
        
        ap_args = []
        g_var = []
        m_var = []
        for arg in args['ap_args']:
            try:
                spec = parse_argparse_arg(s, arg)
            except ParseArgsError as pae:
                print(pae)
                continue
            ap_args.append(spec)
        if g_opt > 0:
            if convert_g in {'y', 'yes'}:
                for arg in args['g_var']:
                    try:
                        specs = parse_assign(s, arg)
                    except ParseArgsError as pae:
                        print(pae)
                        continue
                    for spec in specs:
                        g_var.append(spec)
        if m_opt > 0:
            if convert_m in {'y', 'yes'}:
                for arg in args['m_var']:
                    try:
                        specs = parse_assign(s, arg)
                    except ParseArgsError as pae:
                        print(pae)
                        continue
                    for spec in specs:
                        m_var.append(spec)
        # annotate script with class Args
        # and optionally import argparse, 
        # add new arguments to parser,
        # and/or edit main variables to take input from parser.parse_args()
        edit_script(Path(s), ap_args, g_var, m_var, args["parser"])
