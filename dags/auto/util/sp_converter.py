import ast


def convert_pandas_str(op):
    sozluk = {ast.Add: "+",
              ast.Mult: "*",
              ast.Div: "/",
              ast.Sub: "-",
              ast.In: 'in',
              ast.Gt: ">",
              ast.Lt: "<",
              ast.And: "&",
              ast.LtE: "<=",
              ast.GtE: ">=",
              ast.Or: "|",
              ast.Not: '~',
              ast.Eq: "==",
              ast.NotEq: "!=",
              ast.BitOr: "|",
              ast.BitAnd: "&"
              }

    return sozluk[type(op)]


def convert_spark_str(op):
    sozluk = {ast.Add: "+",
              ast.Mult: "*",
              ast.Div: "/",
              ast.Sub: "-",
              ast.In: 'in',
              ast.Gt: ">",
              ast.Lt: "<",
              ast.And: "AND",
              ast.LtE: "<=",
              ast.GtE: ">=",
              ast.Or: "OR",
              ast.Not: 'NOT',
              ast.Eq: "==",
              ast.NotEq: "!=",
              ast.BitOr: "|",
              ast.BitAnd: "&"
              }

    return sozluk[type(op)]


def ast_to_pandas_code(component, df_id):
    if isinstance(component, ast.Module):
        return '\n'.join(ast_to_pandas_code(n, df_id) for n in component.body).strip()
    elif isinstance(component, ast.FunctionDef):
        args = ', '.join(arg.arg for arg in component.args.args)
        body = ast_to_pandas_code(component.body[0], df_id).rstrip()
        return f"def {component.name}({args}):\n    {body}\n"
    elif isinstance(component, ast.Expr):
        return f"{ast_to_pandas_code(component.value, df_id).strip()}\n"
    elif isinstance(component, ast.Call):
        if hasattr(component.func, "id") and component.func.id == "col":
            return f"{df_id}['{component.args[0].s}']"
        else:
            args = ', '.join(ast_to_pandas_code(arg, df_id).rstrip() for arg in component.args)
            return f"{ast_to_pandas_code(component.func, df_id).rstrip()}({args})"
    elif isinstance(component, ast.Attribute):
        if hasattr(component.value, "id"):
            return f"{component.value.id}.{component.attr}"
        else:
            return f"{ast_to_pandas_code(component.value, df_id)}.{component.attr}"

    elif isinstance(component, ast.Str):
        return f"'{component.s}'"
    elif isinstance(component, ast.Name):
        return f"{component.id}"
    elif isinstance(component, ast.NameConstant):
        return f"{component.value}"
    elif isinstance(component, ast.UnaryOp):
        return f"{convert_pandas_str(component.op)} {ast_to_pandas_code(component.operand, df_id)}"
    elif isinstance(component, ast.BinOp):
        return f"({ast_to_pandas_code(component.left, df_id)}) {convert_pandas_str(component.op)} ({ast_to_pandas_code(component.right, df_id)})"
    elif isinstance(component, ast.Constant):
        return f"{component.n}"
    elif isinstance(component, ast.Num):
        return f'{component.n}'
    elif isinstance(component, ast.Subscript):
        return f"{component.value.id}['{component.slice.value.s}']"
    elif isinstance(component, ast.Compare):
        comps = ', '.join(ast_to_pandas_code(comp, df_id).rstrip() for comp in component.comparators)
        return f"{ast_to_pandas_code(component.left, df_id)} {convert_pandas_str(component.ops[0])} {comps}"
    elif isinstance(component, ast.BoolOp):
        return f"({ast_to_pandas_code(component.values[0], df_id)} ) {convert_pandas_str(component.op)} ( {ast_to_pandas_code(component.values[1], df_id)})"
    elif isinstance(component, ast.List):
        items = ','.join(ast_to_pandas_code(item, df_id).rstrip() for item in component.elts)
        return f"[{items}]"
    else:
        raise TypeError(f"Unhandled component type: {type(component)}")


def spark_to_pandas_filter(code, df_id):
    tree = ast.parse(code)
    result = ast_to_pandas_code(tree, df_id)
    return f"( {result} )"


def ast_to_spark_code(component):
    if isinstance(component, ast.Module):
        return '\n'.join(ast_to_spark_code(n) for n in component.body).strip()
    elif isinstance(component, ast.FunctionDef):
        args = ', '.join(arg.arg for arg in component.args.args)
        body = ast_to_spark_code(component.body[0]).rstrip()
        return f"def {component.name}({args}):\n    {body}\n"
    elif isinstance(component, ast.Expr):
        return f"{ast_to_spark_code(component.value).strip()}\n"
    elif isinstance(component, ast.Call):
        if hasattr(component.func, "id") and component.func.id == "col":
            return f"{component.args[0].s}"
        else:
            args = ', '.join(ast_to_spark_code(arg).rstrip() for arg in component.args)
            return f"{ast_to_spark_code(component.func).rstrip()}({args})"
    elif isinstance(component, ast.Attribute):
        if hasattr(component.value, "id"):
            return f"{component.value.id}.{component.attr}"
        else:
            return f"{ast_to_spark_code(component.value)}.{component.attr}"

    elif isinstance(component, ast.Str):
        return f"'{component.s}'"
    elif isinstance(component, ast.Name):
        return f"{component.id}"
    elif isinstance(component, ast.NameConstant):
        return f"{component.value}"
    elif isinstance(component, ast.UnaryOp):
        return f"{convert_spark_str(component.op)} {ast_to_spark_code(component.operand)}"
    elif isinstance(component, ast.BinOp):
        return f"({ast_to_spark_code(component.left)}) {convert_spark_str(component.op)} ({ast_to_spark_code(component.right)})"
    elif isinstance(component, ast.Constant):
        return f"{component.n}"
    elif isinstance(component, ast.Num):
        return f'{component.n}'
    elif isinstance(component, ast.Subscript):
        return f"{component.value.id}['{component.slice.value.s}']"
    elif isinstance(component, ast.Compare):
        comps = ', '.join(ast_to_spark_code(comp).rstrip() for comp in component.comparators)
        return f"{ast_to_spark_code(component.left)} {convert_spark_str(component.ops[0])} {comps}"
    elif isinstance(component, ast.BoolOp):
        return f"({ast_to_spark_code(component.values[0])} ) {convert_spark_str(component.op)} ( {ast_to_spark_code(component.values[1])})"
    elif isinstance(component, ast.List):
        items = ','.join(ast_to_spark_code(item).rstrip() for item in component.elts)
        return f"[{items}]"
    else:
        raise TypeError(f"Unhandled component type: {type(component)}")


def spark_to_spark_filter(code):
    tree = ast.parse(code)
    result = ast_to_spark_code(tree)
    return f"( {result} )"


if __name__ == '__main__':
    # code = 'col("name") == "sezin" and col("yas") < 30'
    code = 'col("name") == True'

    result = spark_to_pandas_filter(code, "df")
    print(result)
