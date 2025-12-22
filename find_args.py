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

def parse_assign(stmt: ast.Assign):
    print(ast.dump(stmt, indent=4))


def parse_argparse_arg(stmt: ast.Call):
    print(ast.dump(stmt, indent=4))


if __name__ == "__main__":
    scripts = {}
    for script in Path("dummies").iterdir():
        # only parse Python scripts (might extend to .sh later)
        if not script.name.endswith('.py'):
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
            parse_argparse_arg(arg)
        if g_opt > 0:
            if convert_g in {'y', 'yes'}:
                for arg in args['g_var']:
                    parse_assign(arg)
        if m_opt > 0:
            if convert_m in {'y', 'yes'}:
                for arg in args['m_var']:
                    parse_assign(arg)
        # annotate script with class Args
        # and optionally import argparse, 
        # add new arguments to parser,
        # and/or edit main variables to take input from parser.parse_args()
        # (also need to find variable that holds parser.parse_args())
        # edit_script(args)
