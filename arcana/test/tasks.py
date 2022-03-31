from pathlib import Path
import attr
from pydra import mark



@mark.task
def add(a: float, b: float) -> float:
    return a + b


@mark.task
@mark.annotate({
    'dpath': Path,
    'fname': str,
    'return': {
        'path': str,
        'suffix': str}})
def path_manip(dpath, fname):
    path = dpath / fname
    return str(path), path.suffix


@attr.s(auto_attribs=True)
class A():
    x: int
    y: int


@attr.s(auto_attribs=True)
class B():
    u: float
    v: float


@attr.s(auto_attribs=True)
class C():
    z: float


@mark.task
@mark.annotate({
    'a': A,
    'b': B,
    'return': {
        'c': C}})
def attrs_func(a, b):
    return C(z=a.x * b.u + a.y * b.v)


@mark.task
@mark.annotate({
    'return': {'out_file': Path}})
def concatenate(in_file1: Path, in_file2: Path, out_file: Path=None,
                duplicates: int=1) -> Path:
    """Concatenates the contents of two files and writes them to a third

    Parameters
    ----------
    in_file1 : Path
        A text file
    in_file2 : Path
        Another text file
    out_file : Path
       The path to write the output file to 

    Returns
    -------
    Path
        A text file made by concatenating the two inputs
    """
    if out_file is None:
        out_file = Path('out_file.txt').absolute()
    contents = []
    for _ in range(duplicates):
        for fname in (in_file1, in_file2):
            with open(fname) as f:
                contents.append(f.read())
    with open(out_file, 'w') as f:
        f.write('\n'.join(contents))
    return out_file

