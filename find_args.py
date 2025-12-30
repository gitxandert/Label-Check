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


def parse_assign(path: Path, stmt: ast.Assign):
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
        kind = "list"
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
    print(f"\n{path.name} Arg:")
    for name in names:
        sp = ArgSpec(
                name=name,
                kind=kind,
                required=True, # default True, since hardcoded
                form=form,
                default=value,
                desc=None,
            )

        print(f"\tname={sp.name}")
        print(f"\tkind={sp.kind}")
        print(f"\tform={sp.form}")
        print(f"\tdefault={sp.default}\n")
        specs.append(sp)

    # return specs

def parse_argparse_arg(path: Path, stmt: ast.Call):
    print(ast.dump(stmt, indent=4))
    # just use the first name
    name = stmt.args[0]
    if isinstance(name, ast.Constant):
        # get rid of dashes
        while name[0] == '-':
            name = name.lstrip('-')
    else:
        raise ParseArgsError(
                "rvalues must be constants",
                path=path,
                lineno=stmt.args.lineno,
                offset=stmt.args.col_offset,
            )

    kind = None
    required = False
    form = None
    default = None
    desc = None
    store_const = False

    for kw in stmt.keywords:
        match kw.arg:
            case 'type':
                kind = kw.value.id
                break
            case 'required':
                required = kw.value.value
                break
            case 'help':
                if not isinstance(kw.value, ast.JoinedStr):
                    desc = kw.value.value
                break
            case 'action':
                if isinstance(kw.value, ast.Constant):
                    match kw.value.value:
                        case 'store_true':
                            kind = 'bool'
                            default = 'True'
                            break
                        case 'store_false':
                            kind = 'bool'
                            default = 'False'
                            break
                        case 'store':
                            # default behavior
                            break
                        case 'store_const':
                            # should be accompanied with
                            # const argument
                            store_const = True
                            break
                        case _:
                            raise ParseArgsError(
                                    "Complicated or incorrectly formed argument",
                                    path=path,
                                    lineno=kw.lineno,
                                    offset=kw.col_offset,
                                )
                break
            case 'nargs':
                # if nargs is a constant, then format is a tuple,
                # and metavar argument must be provided
                break
            case 'const':
                default = kw.value.value
                break
            case 'choices':
                break
            case 'default':
                break
            case _:
                break
    
    # ensure type argument was provided
    if kind = None:
        raise ParseArgsError(
                "type argument is required",
                path=path,
                lineno=stmt.keywords.lineno,
                offset=stmt.keywords.col_offset,
            )

    sp = ArgSpec(
            name=name,
            kind=kind,
            required=required,
            form=form,
            default=value,
            desc=desc,
        )

    print(f"\tname={sp.name}")
    print(f"\tkind={sp.kind}")
    print(f"\tform={sp.form}")
    print(f"\tdefault={sp.default}\n")



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
        # The first two groups will be signalled to the user
        # as able to be optionally-configured into variables
        # stored in a global .runner.config file; this allows users
        # to forego hardcoding variables altogether.
        # Arguments parsed by argparse will be automatically
        # included in class Args.
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
        for arg in args['ap_args']:
            parse_argparse_arg(s, arg)
        if g_opt > 0:
            if convert_g in {'y', 'yes'}:
                for arg in args['g_var']:
                    try:
                        parse_assign(s, arg)
                    except ParseArgsError as pae:
                        print(pae)
                        continue
        if m_opt > 0:
            if convert_m in {'y', 'yes'}:
                for arg in args['m_var']:
                    try:
                        parse_assign(s, arg)
                    except ParseArgsError as pae:
                        print(pae)
                        continue
        # annotate script with class Args
        # and optionally import argparse, 
        # add new arguments to parser,
        # and/or edit main variables to take input from parser.parse_args()
        # (also need to find variable that holds parser.parse_args())
        # edit_script(args)
