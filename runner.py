"""
This program allows for both seamless running of scripts in sequence (i.e. can run each script one after the other without user intervention) and intermittent processing that pauses between scripts to notify the user of errors and/or ask for permission to continue. It replaces providing multiple command line arguments at runtime with a syntax class parsing system, in which each script is annotated with a class Args that details all of the required and optional arguments to run that script. A config file is generated that contains all of the scripts (and their accompanying arguments) to be run when this program is invoked from the command line.

Instead of parsing multiple command line arguments, this program parses a single .runner.config file in the directory from which this script is called. Running this program with the '--config' flag brings up an interface through which the user can schedule scripts and configure their arguments within .runner.config. (Note: running with '--config' ignores all other arguments.) If there is no .runner.config, the program will generate one and prompt the user to select scripts and provide arguments to run these scripts in sequence. Generating the config file with `runner.py --config` will ensure that the program cannot be run until all of the scripts are syntactically and validly annotated, and that the user has provided all of the required arugments. If the user generates the .runner.config file manually, then if an argument that is necessary to run any of the scripts is missing, the program will terminate before any scripts are run and specify what is necessary to run the pipeline.

Any script that can be run by this program should be placed in the scripts/ directory. When this program is run with '--config', it checks all of the scripts in this directory for the syntax class Args and parses arguments for every script with this annotation. If a script does not have this annotation, the user will be prompted to create one; however, the user is only obligated to provided annotations for scripts that are scheduled to be run. 

The annotation should be formatted in the following style (an argument's name does not need to be formatted according to any certain convention):
    
    class Args:
        arg_desc: Arg.desc("This description describes the purpose and function of the script; it is purely annotative")
        arg_str: Arg.str(desc="A string argument", default="Hello runner.py")
        arg_int: Arg.int(desc="An integer argument", default=1)
        arg_float: Arg.float(desc="A float argument", default=1.001)
        arg_path: Arg.path(desc="A file/directory path", required=True)
        arg_tuple: Arg.tuple(desc="Any kind of formatted tuple", form=(int, int), default=(0,0))
        arg_list: Arg.list(desc="A list of values", form=[str], default=["svs", "tif", "ndpi"])

Each argument's value must be declared with Arg.[type]() (all of the allowed types are listed above). For most types, no subargument is required: the name and type is enough to specify the argument. However, if an argument is an Arg.tuple or Arg.list, the form argument must be provided; e.g. if an argument expects a tuple of (int, str), it should be an Arg.tuple(form=(int, str)); if an argument expects a list of floats, it should be an Arg.list(form=[float]). Any fault in formatting will be brought to the user's attention, and the program will not run until it is rectified.

Rather than manually formatting a class Args in each script, the user should instead generate this class with `runner.py --config`. If a script in scripts/ does not have a class Args, the program will prompt the user to create one interactively, ensure formatting step-by-step, and annotate the script with this class after approval. However, if the user decides to manually format the class, `runner.py --config` will still validate the annotation and bring to light any syntax and/or value errors.

Scripts with valid Args annotations can be scheduled to run with `runner.py --config`. Valid scripts are selected to run in a specific order, and the arguments necessary to run each script will be saved to .runner.config. If a script already has an entry in .runner.config, the user will be prompted to edit it for a specific run, if they choose; if a script does not have an entry in .runner.config, the user must supply at least the required arguments to run this script. (If a script does not require any arguments to run, then the annotation need only include an Arg.desc() argument, to describe the function and purpose of the script.) When the program runs normally (i.e. without the --config flag), it will look in .runner.config for all of the scripts it is to run and will run each script sequentially in subprocess(), with its configured arguments. 

Although --config makes certain that the annotations are syntactically correct and valid, and that all required arguments are validated and stored in .runner.config, this does not guarantee that the scripts will accept the provided arguments, if the Args annotation allows for incorrect arguments. The user should understand exactly what types each argument to a script requires, and format the Args annotation accordingly.

To run uninterruptedly, the user simply invokes `runner.py` with no arguments. 
To pause between scripts, the user includes the '--pause' flag. 
To run from a certain script in the pipeline, the user specifies this script with the '--from' argument.
To see logs recorded from the last x runs, the user runs the program with '--logs x'.
"""

import os, sys
import ast
import argparse
from pathlib import Path
from typing import Optional, Any
from dataclasses import dataclass

#---------------- ARGS PARSING ----------------#

# ArgSpec: dataclass to store details for singular argument
# Args can be of the following types:
# - str
# - path
# - int
# - float
# - tuple
# - list
# - desc
# (desc is a str that describes the argument; 
# it does not get applied to the argument.)
@dataclass(frozen=True)
class ArgSpec:
    name: str               # required (inferred from annotation)
    kind: str               # required (inferred from annotation)
    required: bool          # required (default False)
    form: Any | None      # required for Arg.tuple and Arg.list
    default: Any | None
    desc: str | None


# data class to store all arguments for a single script
@dataclass(frozen=True)
class ScriptArgs:
    name: str
    desc: str | None
    required: dict[str, ArgSpec]
    optional: dict[str, ArgSpec]


# error handling for parsing syntax class Args
class ArgsSyntaxError(Exception):
    def __init__(self, message: str, path=None, lineno=None, offset=None):
        self.path = path
        self.lineno = lineno

        prefix = ""
        if path is not None:
            prefix += str(path)
        if lineno is not None:
            prefix += f":{lineno}"
            if offset is not None:
                prefix += f":{offset}"
        if prefix:
            prefix += ": "

        super().__init__(prefix + message)


# signals that there is no class Args in the script
class MissingArgsError(Exception):
    def __init__(self):
        super().__init__()


# throws when the user declines to fix or init class Args
class ParseArgsException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


# retry an operation (usually after some required configuring)
class RetryException(Exception):
    def __init__(self):
        super().__init__()


def extract_args_node(script_path: Path) -> ast.ClassDef:
    tree = ast.parse(script_path.read_text())
    
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "Args":
            return node

    raise MissingArgsError()


def validate_default(path: Path, stmt: ast.Call, ty: str, default: Any, form: Any = None):
    match ty:
        case 'int':
            if not isinstance(default, int):
                raise ArgsSyntaxError(
                        f"{default} is not an integer",
                        path=path,
                        lineno=stmt.lineno,
                        offset=stmt.col_offset,
                    )
        case 'float':
            if not isinstance(default, float):
                raise ArgsSyntaxError(
                        f"{default} is not a float",
                        path=path,
                        lineno=stmt.lineno,
                        offset=stmt.col_offset,
                    )
        case 'tuple':
            for i, ty in enumerate(form):
                validate_default(path, stmt, ty, default[i])
        case 'list':
            for item in default:
                validate_default(path, stmt, form[0], item)
        case _:
            # str and path can be pretty much be anything, so pointless to validate
            pass


def parse_tuple(arg: str, kw: ast.keyword, path: Path) -> tuple:
    match arg:
        case 'default':
            if isinstance(kw.value, ast.Tuple):
                elts = kw.value.elts

                values = []
                for e in elts:
                    if isinstance(e, ast.Constant):
                        values.append(e.value)
                    else:
                        raise ArgsSyntaxError(
                            "default arguments must be literals",
                            path=path,
                            lineno=e.value,
                            offset=e.value.col_offset,
                        )

                return tuple(values)
            else:
                raise ArgsSyntaxError(
                    f"{arg} must be a tuple (x, y, ...)",
                    path=path,
                    lineno=kw.value.lineno,
                    offset=kw.value.col_offset,
                )
        case 'form':
            if isinstance(kw.value, ast.Tuple):
                elts = kw.value.elts

                values = []
                for e in elts:
                    if isinstance(e, ast.Name):
                        if e.id in {'str', 'int', 'float', 'Path'}:
                            values.append(e.id)
                        else:
                            raise ArgsSyntaxError(
                                    f"Unknown type '{e.id}",
                                    path=path,
                                    lineno=e.lineno,
                                    offset=e.col_offset,
                                )
                    else:
                        raise ArgsSyntaxError(
                            "form arguments must be type names",
                            path=path,
                            lineno=e.value,
                            offset=e.value.col_offset,
                        )

                return tuple(values)
            else:
                raise ArgsSyntaxError(
                    f"{arg} must be a tuple (x, y, ...)",
                    path=path,
                    lineno=kw.value.lineno,
                    offset=kw.value.col_offset,
                )


def parse_list(arg: str, kw: ast.keyword, path: Path) -> list:
    match arg:
        case 'default':
            if isinstance(kw.value, ast.List):
                elts = kw.value.elts

                values = []
                for e in elts:
                    if isinstance(e, ast.Constant):
                        values.append(e.value)
                    else:
                        raise ArgsSyntaxError(
                            "default arguments must be literals",
                            path=path,
                            lineno=e.value,
                            offset=e.value.col_offset,
                        )

                return values 
            else:
                raise ArgsSyntaxError(
                    f"{arg} must be a list [x, y, ...]",
                    path=path,
                    lineno=kw.value.lineno,
                    offset=kw.value.col_offset,
                )
        case 'form':
            if isinstance(kw.value, ast.List):
                elts = kw.value.elts

                values = []
                if len(elts) > 1:
                    raise ArgsSyntaxError(
                            "lists can only be of one type",
                            path=path,
                            lineno=kw.value.lineno,
                            offset=kw.value.col_offset,
                        )
                
                e = elts[0]
                if isinstance(e, ast.Name):
                    if e.id in {'str', 'int', 'float', 'Path'}:
                        return  '[' + e.id + ']'
                    else:
                        raise ArgsSyntaxError(
                                f"Unknown type '{e.id}",
                                path=path,
                                lineno=e.lineno,
                                offset=e.col_offset,
                            )
                else:
                    raise ArgsSyntaxError(
                        "form argument must be a type name",
                        path=path,
                        lineno=e.value,
                        offset=e.value.col_offset,
                    )
            else:
                raise ArgsSyntaxError(
                    f"{arg} must be a list[x, y, ...]",
                    path=path,
                    lineno=kw.value.lineno,
                    offset=kw.value.col_offset,
                )


def parse_arg_spec(name: str, value: ast.Call, path: Path) -> ArgSpec:
    required = False
    desc = None
    form = None
    default = None

    keywords = []
    specs = {}

    kind = value.func
    match kind.attr:
        case 'desc':
            if value.keywords:
                raise ArgsSyntaxError(
                        "Arg.desc() does not take keywords",
                        path=path,
                        lineno=value.keywords.lineno,
                        offset=value.keywords.offset,
                    )

            if len(value.args) != 1:
                raise ArgsSyntaxError(
                        "Arg.desc() requires exactly one argument",
                        path=path,
                        lineno=value.lineno,
                        offset=value.col_offset,
                    )
            
            arg = value.args[0]
            if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
                raise ArgsSyntaxError(
                        "Arg.desc() argument must be a string literal",
                        path=path,
                        lineno=arg.lineno,
                        offset=arg.col_offset,
                    )
            
            desc = arg.value
        case 'str' | 'int' | 'float' | 'path' | 'tuple' | 'list':
            if not value.keywords:
                raise ArgsSyntaxError(
                        f"Arg.{kind.attr}() requires keywords",
                        path=path,
                        lineno=value.lineno,
                        offset=value.col_offset,
                    )

            keywords = value.keywords
        case _:
            raise ArgsSyntaxError(
                    "Unrecognized Arg type",
                    path=path,
                    lineno=kind.lineno,
                    offset=kind.col_offset,
                )
    
    for kw in keywords:
        arg = kw.arg
        match arg:
            case 'required' | 'desc' | 'form' | 'default':
                pass
            case _:
                raise ArgsSyntaxError(
                        f"Invalid keyword '{arg}'",
                        path=path,
                        lineno=kw,
                        offset=kw.col_offset,
                    )

        match kind.attr:
            case 'str' | 'int' | 'float' | 'path':
                if isinstance(kw.value, ast.Constant):
                    val = kw.value.value
                else:
                    raise ArgsSyntaxError(
                        f"Argument must be a literal",
                        path=path,
                        lineno=kw.value.lineno,
                        offset=kw.value.col_offset,
                    )

                # validate_val(val, arg, kind.attr, path)

                specs[arg] = val
            case 'tuple':
                match arg:
                    case 'default' | 'form':
                        val = parse_tuple(arg, kw, path)
                    case _:
                        if isinstance(kw.value, ast.Constant):
                            val = kw.value.value
                        else:
                            raise ArgsSyntaxError(
                                f"Argument must be a literal",
                                path=path,
                                lineno=kw.value.lineno,
                                offset=kw.value.col_offset,
                            )


                specs[arg] = val
            case 'list':
                match arg:
                    case 'default' | 'form':
                        val = parse_list(arg, kw, path)
                    case _:
                        if isinstance(kw.value, ast.Constant):
                            val = kw.value.value
                        else:
                            raise ArgsSyntaxError(
                                f"Argument must be a literal",
                                path=path,
                                lineno=kw.value.lineno,
                                offset=kw.value.col_offset,
                            )
                specs[arg] = val

    form = specs.get('form', form)
    if kind.attr in {'tuple', 'list'}:
        if form is None:
            raise ArgsSyntaxError(
                    f"{kind.attr} requires a form argument",
                    path=script_path,
                    lineno=value.lineno,
                    offset=value.col_offset,
                )
        
    default = specs.get('default', default)
    if default is not None:
        validate_default(path, value, kind.attr, default, form)

    required = specs.get('required', required)
    desc = specs.get('desc', desc)
      
    return ArgSpec(
            name=name,
            kind=kind.attr,
            required=required,
            desc=desc,
            form=form,
            default=default,
        )


def parse_node(class_node: ast.ClassDef, script_path: Path) -> ScriptArgs:
    specs = {}
    for stmt in class_node.body:
        if not isinstance(stmt, ast.Assign):
            continue

        if len(stmt.targets) != 1 or not isinstance(stmt.targets[0], ast.Name):
            raise ArgsSyntaxError(
                    "Invalid Args assignment",
                    path=script_path,
                    lineno=stmt.lineno,
                    offset=stmt.col_offset,
                )
        
        value = stmt.value
        if value.func.value.id != "Arg":
            raise ArgsSyntaxError(
                    "Arg value must be formatted as Arg.[type](...)",
                    path=script_path,
                    lineno=value.lineno,
                    offset=value.col_offset,
                )

        name = stmt.targets[0].id

        # propagates ArgsSyntaxError up to parse_script_args
        spec = parse_arg_spec(name, value, script_path)

        specs[name] = spec
    
    name = script_path.name
    spdesc = specs.get("desc", None)
    if spdesc is not None:
        desc = spdesc.desc
    else:
        desc = None
    required = {}
    optional = {}

    for spec in specs.values():
        if spec.required == True:
            required[spec.name] = spec
        else:
            optional[spec.name] = spec

    return ScriptArgs(
            name=name,
            desc=desc,
            required=required,
            optional=optional,
        )


# consider returning ArgSpecs/ScriptArgs
def search_for_args(s: Path) -> list[str]:
    pass


# optionally configures list of args found in search_for_args
def create_new_args(s: Path, args: list[str] = None):
    pass


# edit class Args for a script
def edit_class_args(s: Path):
    pass


def parse_script_args(s: Path) -> ScriptArgs:
    try:
        node = extract_args_node(s)
    except MissingArgsError:
        print(f"{s} doesn't have a class Args.")
        create = input(f"Create class Args for {s}? [y/N] ")
        create = create.strip().lower()
        if create:
            potential_args = search_for_args(s)
            if potential_args = "":
                print(f"Couldn't find any potential args for {s} globally or in a 'main'.")
                do_it = input(f"Create class Args for {s} anyway? [y/N] ")
            else:
                print(f"Found the following potential args for {s}:")
                for pa in potential_args:
                    print(f"\t{pa}")
                do_it = input(f"Create class Args for {s} out of the above? [y/N] ")
            do_it = do_it.strip().lower()
            if do_it:
                create_new_args(s, potential_args)
                # signal to retry this function 
                # with newly-configured class Args
                raise RetryException()
            else:
                raise ParseArgsException(
                        f"Cannot add {s} to pipeline without class Args.\n"
                    )
        else:
            raise ParseArgsException(
                    f"Cannot add {s} to pipeline without class Args.\n"
                )

    try:
        args = parse_node(node, s)
    except ArgsSyntaxError as ase:
        print(f"Error: {ase}")
        fix = input(f"Fix args for {s}? [y/N] ")
        if fix:
            edit_class_args(s)
        else:
            raise ParseArgsException(
                    f"Cannot add {s} to pipeline without correctly-configured class Args.\n"
                )
   
   return args
"""
    for sc, ar in scripts.items():
        print(sc)
        if ar.desc is not None:
            print(f"\t{ar.desc}")
        print(f"\tRequired args:")
        for spec, args in ar.required.items():
            print(f"\t\t{spec}")
            if args.desc is not None:
                print(f"\t\t\tdesc -- {args.desc}")
            print(f"\t\t\ttype -- {args.kind}")
            if args.form is not None:
                print(f"\t\t\tform -- {args.form}")
            if args.default is not None:
                print(f"\t\t\tdefault -- {args.default}")
        print(f"\tOptional args:")
        for spec, args in ar.optional.items():
            if spec != 'desc':
                print(f"\t\t{spec}")
                if args.desc is not None:
                    print(f"\t\t\tdesc -- {args.desc}")
                print(f"\t\t\ttype -- {args.kind}")
                if args.form is not None:
                    print(f"\t\t\tform -- {args.form}")
                if args.default is not None:
                    print(f"\t\t\tdefault -- {args.default}")
"""


# prints list of script names and returns list of script paths
# optionally enumerates scripts (so that they can be selected)
def list_scripts(enum: bool = False) -> list[Path]:
    scripts = []
    count = 0
    for p in Path("scripts").iterdir():
        # ignore Vim stuff
        if not p.name.endswith('~') and not p.name.endswith('.swp'):
            scripts.append(p)
            count += 1
            if enum:
                print(f"{count}) {p.name}")
            else:
                print(p.name)
    print("")

    return scripts


#---------------- VARIABLES ----------------#


# catch errors with variable creation
class VariableError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


def validate_variable(ty: str, val: str, form: str | None = None) -> str:
    match ty:
        case 'int':
            try:
                v = int(val)
            except:
                raise VariableError(f"{val} is not a valid {ty}.\n")
            return val
        case 'float':
            try:
                v = float(val)
            except:
                raise VariableError(f"{val} is not a valid {ty}.\n")
            return val
        case 'tuple':
            if form is not None:
                types = form.lstrip("(").rstrip(")").split(", ")
                vals = val.lstrip("(").rstrip(")").split(", ")
                for i, t in enumerate(types):
                    validate_variable(t, vals[i])
                val = "("
                for i, v in enumerate(vals):
                    if i < len(vals) - 1:
                        val += f"{v}, "
                    else:
                        val += f"{v})"
                return val
            else:
                raise VariableError("tuple requires form.\n")
        case 'list':
            if form is not None:
                t = form.lstrip("[").rstrip("]")
                vals = val.lstrip("[").rstrip("]").split(", ")
                for v in vals:
                    validate_variable(t, v)
                val = "["
                for i, v in enumerate(vals):
                    if i < len(vals) - 1:
                        val += f"{v}, "
                    else:
                        val += f"{v}]"
                return val
            else:
                raise VariableError("list requires form.\n")
        case _:
            # format as str
            return str(val)


def validate_tuple_format(form: str) -> str:
    types = form.strip().lstrip("(").rstrip(")").split(", ")
    for t in types:
        if t not in {"str", "path", "int", "float"}:
            raise VariableError(f"{t} is not a valid type.\n")
    # if valid, reformat tuple string
    form = "("
    for i, t in enumerate(types):
        if i != len(types) - 1:
            form += f"{t}, "
        else:
            form += f"{t})"
    return form


def validate_list_format(form: str) -> str:
    t = form.strip().lstrip("[").rstrip("]")
    if t not in {"str", "path", "int", "float"}:
        raise VariableError("Error: {t} is not a valid type.\n")
    # if valid, reformat list string
    form = f"[{t}]"
    return form


def edit_variables():
    try:
        with open(".runner.var", 'r') as f:
            var = f.read()
    except FileNotFoundError:
        print("No .runner.var in this directory.")
        create = input("Create a new .runner.var? [y/N] ")
        if create:
            with open(".runner.var", 'w') as f:
                f.write("")
            var = ""
        else:
            return
    except Exception as e:
        print(e, file=sys.stderr)
        return
    
    print("")
    while True:
        if var == "":
            print("No variables in .runner.var.\n")
        else:
            print(var)
        option = input("[a]dd variable | [e]dit variable | [d]elete variable | [c]ommit changes | [r]eturn to config: ")
        option = option.strip().lower()
        if option in {'a', "add", "add variable"}:
            name = input("Enter variable name: ")
            name = name.strip()
            if name == "":
                print("Error: must enter valid name.\n")
                continue

            lines = var.split('\n')
            for line in lines:
                line = line.split('=')[0]
            if name in lines:
                print(f"Error: '{name}' already exists.\n")

            ty = input("Enter variable type (str, path, int, float, tuple, list): ")
            ty = ty.strip()
            if ty not in {"str", "path", "int", "float", "tuple", "list"}:
                print(f"Error: {ty} is not a valid variable type.\n")
                continue

            form = None
            if ty == "tuple":
                form = input(f"Enter tuple format (e.g. (str, int)): ")
                form = form.strip()
                try:
                    form = validate_tuple_format(form)
                except VariableError as ve:
                    print(f"Error: {ve}")
                    continue

            if ty == "list":
                form = input(f"Enter list format (e.g. [str]): ")
                form = form.strip()
                try:
                    form = validate_list_format(form)
                except VariableError as ve:
                    print(f"Error: {ve}")
                    continue

            val = input(f"Enter {ty} value: ")
            val = val.strip()

            try:
                val = validate_variable(ty, val, form)
            except VariableError as ve:
                print(f"Error: {ve}")
                continue

            if form is not None:
                ty += f": {form}"
            var += f"{name}={val}\t{ty}\n"
            print(f"Will add {name} to .runner.var.\n")
        elif option in {'e', "edit", "edit variable"}:
            name = input("Enter variable name: ")
            name = name.strip()
            lines = var.split('\n')
            variables = {}
            found = False
            for line in lines:
                if line == "":
                    continue
                v = ""
                for ch in line:
                    if ch != '=':
                        v += ch
                    else:
                        break
                if v == name:
                    found = True
                variables[v] = line
            if not found:
                print(f"Error: no variable named {name}.\n")
                continue

            confirm = input(f"Edit {name}? [y/N] ")
            confirm = confirm.strip().lower()
            if confirm in {'y', "yes"}:
                val_and_type = variables[name].split('=')[1].split('\t')
                new_name = name
                new_val = val_and_type[0]
                new_type = val_and_type[1]

                edit_name = input(f"Edit {name}'s name? [y/N] ")
                edit_name = edit_name.strip().lower()
                if edit_name in {'y', "yes"}:
                    n_n = input(f"Enter new name for {name}: ")
                    n_n = n_n.strip()
                    if n_n in variables.keys():
                        print("Error: {n_n} already exists.\n")
                        continue
                    new_name = n_n

                edit_type = input(f"Edit {name}'s type? [y/N] ")
                edit_type = edit_type.strip().lower()
                if edit_type in {'y', "yes"}:
                    n_t = input(f"Enter new type for {name} (str, path, int, float, tuple, list): ")
                    n_t = n_t.strip()
                    if n_t not in {"str", "path", "int", "float", "tuple", "list"}:
                        print(f"Error: {n_t} is not a valid type.\n")
                        continue
                    new_type = n_t

                new_form = None
                # new_type won't match against the following
                # if ! edit_type because type is formatted as 
                # e.g. "tuple: (int, int)" or "list: [str]"
                if new_type in {"tuple", "list"}:
                    n_f = input(f"Enter new form for {name} ({new_type}): ")
                    n_f = n_f.strip()
                    try:
                        if new_type == "tuple":
                            new_form = validate_tuple_format(n_f)
                        else:
                            new_form = validate_list_format(n_f)
                    except VariableError as ve:
                        print(f"Error: {ve}")
                        continue
                
                edit_val = input(f"Edit value for {name}? [y/N] ")
                edit_val = edit_val.strip()
                if edit_val in {'y', "yes"}:
                    n_v = input(f"Enter new value for {name}: ")
                    new_val = n_v.strip()

                try:
                    val = validate_variable(new_type, new_val, new_form)
                except VariableError as ve:
                    print(f"Error: {ve}")
                    continue

                if new_form is not None:
                    new_type += f": {new_form}"

                var = ""
                for v, line in variables.items():
                    if v == name:
                        var += f"{new_name}={new_val}\t{new_type}\n"
                    else:
                        var += f"{line}\n"
                print(f"Will save '{name}' as '{new_name}'.\n")
        elif option in {'d', "delete", "delete variable"}:
            name = input("Enter variable name: ")
            name = name.strip()
            lines = var.split('\n')
            variables = {}
            found = False
            for line in lines:
                if line == "":
                    continue
                v = ""
                for ch in line:
                    if ch != '=':
                        v += ch
                    else:
                        break
                if v == name:
                    found = True
                variables[v] = line
            if not found:
                print(f"Error: no variable named '{name}'.\n")
                continue

            confirm = input(f"Delete {name}? [y/N] ")
            confirm = confirm.strip().lower()
            if confirm in {'y', "yes"}:
                var = ""
                for v, line in variables.items():
                    if v != name:
                        var += f"{line}\n"
            print(f"Will delete {name} from .runner.var.\n")
        elif option in {'c', "commit", "commit changes"}:
            try:
                with open(".runner.var", 'w') as f:
                    f.write(var)
            except Exception as e:
                print(e, file=sys.stderr)
            print("Committed changes to .runner.var.\n")
            break
        elif option in {'r', "return", "return without saving"}:
            print("Returning to config without saving...\n")
            break
        else:
            print(f"Unrecognized option '{option}'.\n")


#---------------- CONFIG ----------------#


# loads arguments from .runner.config
class Config:
    def __init__(self):
        try:
            with open('.runner.config', 'r', encoding='utf-8') as f:
                self.args = self.get_args(f)
        except FileNotFoundError:
            print(f"Error: No .runner.config found in the current directory", file=sys.stderr)
            print("You can create a new .runner.config by running this script with the '--config' flag")
            sys.exit(1)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)


def edit_config():
    try:
        with open(".runner.config", 'r') as f:
            config = f.read()
    except FileNotFoundError:
        print("No .runner.config in this directory.\n")
        create = input("Create a new .runner.config? [y/N] ")
        match create.lower():
            case 'y' | "yes":
                with open(".runner.config", 'w') as f:
                    f.write("")
            case _:
                sys.exit(0)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(0)

    while True:
        if config == "":
            option = input("[n]ew config | list [s]cripts | edit [v]ariables | [q]uit: ")
            option = option.strip().lower()
            if option in {'n', "new", "new config"}:
                listed = list_scripts(enum=True)
                if len(listed) == 0:
                    print("Add scripts to scripts/ to stitch together a pipeline.\n")
                    continue

                pipeline = input("Select scripts to sequence in pipeline: ") 
                script_nums = pipeline.split()
                scripts = []
                for idx in script_nums:
                    try:
                        idx = int(idx)
                    except:
                        print(f"Error: {idx} is not a valid option.\n")
                        continue
                    
                    if idx < 1 or idx > num_scripts:
                        print(f"Error: {idx} is not a valid option.\n")
                        continue
                    
                    scr = listed[idx - 1] # list starts at 1
                    if scr in scripts:
                        print(f"Error: {scr} already selected.\n")
                        continue
                    else:
                        scripts.append(scr)

                script_args = {}
                for s in scripts:
                    try:
                        args = parse_script_args(s)
                    except RetryException as re:
                        # raised if the current script had no class Args,
                        # but class Args was configured after the first pass
                        args = parse_script_args(s)
                    except ParseArgsException as ce:
                        # raised if the user declines to config class Args correctly
                        print(ce)
                        scripts.remove(s)
                        continue
                    
                    script_args[s] = args

            elif option in {'v', "variables", "edit variables"}:
                edit_variables()    
            elif option in {'s', "scripts", "list scripts"}:
                if len(list_scripts()) == 0:
                    print("No scripts in scripts/.\n")
            elif option in {'q', "quit"}:
                print("Quit --config.")
                break
            else:
                print(f"Unrecognized option '{option}'.\n")
        else:
            option = input("[e]dit config | [r]eview config | list [s]cripts | edit [v]ariables | [c]ommit changes | [q]uit: ")
            option = option.lower()
            if option in {'e', "edit", "edit config"}:
                print("Editing config.\n")
            elif option in {'r', "review", "review config"}:
                print(f"\n{config}\n")
            elif option in {'s', "scripts", "list scripts"}:
                if len(list_scripts()) == 0:
                    print("No scripts in scripts/.\n")
            elif option in {'v', "variables", "edit variables"}:
                edit_variables()
            elif option in {'c', "commit", "commit changes"}:
                try:
                    with open(".runner.config", 'w') as f:
                        f.write(config)
                except Exception as e:
                    print(e, file=sys.stderr)
                    continue

                print("Committed changes to .runner.config.\n")
                break
            elif option in {'q', "quit"}:
                print("Quit --config.")
                break
            else:
                print(f"Unrecognized option '{option}'.\n")                   
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from")
    parser.add_argument("--pause", action="store_true")
    parser.add_argument("--config", action="store_true")
    parser.add_argument("--logs", type=int)

    args = parser.parse_args()

    if args.config:
        edit_config()

    # config = Config()
    # print(config.args)
