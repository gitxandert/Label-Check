import os, sys
import ast
from pathlib import Path
from typing import Optional, Any

if __name__ == "__main__":
    for script in Path("dummies").iterdir():
        if script.name.endswith('~') or script.name.endswith('.swp'):
            continue
        print(f"\n----{script}----\n")
        tree = ast.parse(script.read_text())
        body = tree.body
        for stmt in body:
            if isinstance(stmt, ast.Assign):
                print(ast.dump(stmt, indent=4))
                print("")
            if isinstance(stmt, ast.If):
                test = stmt.test
                if test.left.id == "__name__" \
                and isinstance(test.ops[0], ast.Eq) \
                and test.comparators[0].value == "__main__":
                    if_body = stmt.body
                    for if_stmt in if_body:
                        val = if_stmt.value
                        if isinstance(if_stmt, ast.Assign):
                            if not isinstance(val, ast.Call):
                                print(ast.dump(if_stmt, indent=4))
                                print("")
                        elif isinstance(if_stmt, ast.Expr):
                            if isinstance(val, ast.Call):
                                func = val.func
                                if isinstance(func, ast.Attribute):
                                    if func.attr == 'add_argument':
                                        print(ast.dump(val, indent=4))
                                        print("")
