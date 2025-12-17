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

Scripts with valid Args annotations can be scheduled to run with `runner.py --config`. Valid scripts are selected to run in a specific order, and the arguments necessary to run each script will be saved to .runner.config. If a script already has an entry in .runner.config, the user will be prompted to edit it for a specific run, if they choose; if a script does not have an entry in .runner.config, the user must supply at least the required arguments to run this script. When the program runs normally (i.e. without the --config flag), it will look in .runner.config for all of the scripts it is to run and will run each script sequentially in subprocess(), with its configured arguments. 

Although --config makes certain that the annotations are syntactically correct and valid, and that all required arguments are validated and stored in .runner.config, this does not guarantee that the scripts will accept the provided arguments, if the Args annotation allows for incorrect arguments. The user should understand exactly what types each argument to a script requires, and format the Args annotation accordingly.

To run uninterruptedly, the user simply invokes `runner.py` with no arguments. 
To pause between scripts, the user includes the '--pause' flag. 
To run from a certain script in the pipeline, the user specifies this script with the '--from' argument.
To see logs recorded from the last x runs, the user runs the program with '--logs x'.
"""

import os, sys
import ast
import json
import argparse
from pathlib import Path
from typing import Optional, Any
from dataclasses import dataclass

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


def extract_args_node(script_path: Path) -> ast.ClassDef:
    tree = ast.parse(script_path.read_text())
    
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "Args":
            return node

    raise ArgsSyntaxError(
            "Contains no class Args",
            path=script_path,
        )


def parse_tuple(arg, kw, path) -> tuple:
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

                specs[arg] = val
            case 'tuple':
                match arg:
                    case 'default' | 'form':
                        try:
                            val = parse_tuple(arg, kw, path)
                        except Exception as e:
                            print(e, file=sys.stderr)
                            continue
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

    required = specs.get('required', required)
    desc = specs.get('desc', desc)
    form = specs.get('form', form)
    default = specs.get('default', default)
      
    return ArgSpec(
            name=name,
            kind=kind.attr,
            required=required,
            desc=desc,
            form=form,
            default=default,
        )


# eventually return ScriptArgs
def parse_node(class_node: ast.ClassDef, script_path: Path):
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
        kind = value.func.attr

        try:
            spec = parse_arg_spec(name, value, script_path)
        except Exception as e:
            print(e, file=sys.stderr)
            continue

        # need to match against different kinds
        # to process keywords variously
        # (desc doesn't even have any keywords)

        # keywords = [k for k in value.keywords]
        # specs = {}
        # for kw in keywords:
            # spec = kw.arg
            # val = ast.dump(kw.value)
            # specs[spec] = val
        print(f"\t{name}: {kind}")
        print(f"\t\t{spec}")


def process_script_args(scripts_dir: Path):
    # scripts = {}
    for script in scripts_dir.glob("*.py"):
        try:
            node = extract_args_node(script)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            continue

        try:
            args = parse_node(node, script)
        except Exception as e:
            print(e, file=sys.stderr)
            continue

   # print(scripts)

def edit_config():
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

    process_script_args(Path("scripts"))

    # config = Config()
    # print(config.args)
