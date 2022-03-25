
def converter(output_format):
    def decorator(func):
        anot = func.__annotations__
        anot['arcana_converter'] = output_format
        return func
    return decorator