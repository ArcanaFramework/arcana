from pathlib import Path
import pytest
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
        out_file = in_file1.parent / 'out_file.txt'
    contents = []
    for _ in range(duplicates):
        for fname in (in_file1, in_file2):
            with open(fname) as f:
                contents.append(f.read())
    with open(out_file, 'w') as f:
        f.write('\n'.join(contents))
    return out_file


TEST_TASKS = {
    'add': (add,
            {'a': 4,
             'b': 5},
            {'out': 9}),
    'path_manip': (path_manip,
                   {'dpath': Path('/home/foo/Desktop'),
                    'fname': 'bar.txt'},
                   {'path': '/home/foo/Desktop/bar.txt',
                    'suffix': '.txt'}),
    'attrs_func': (attrs_func, 
                   {'a': A(x=2, y=4),
                    'b': B(u=2.5, v=1.25)},
                   {'c': C(z=10)})}

BASIC_TASKS = ['add', 'path_manip', 'attrs_func']

FILE_TASKS = ['concatenate']


@pytest.fixture(params=BASIC_TASKS)
def pydra_task_details(request):
    func_name = request.param
    return ('arcana2.test_fixtures.tasks.' + func_name,) + tuple(
        TEST_TASKS[func_name][1:])
            

@pytest.fixture(params=BASIC_TASKS)
def pydra_task(request):
    task, args, expected_out = TEST_TASKS[request.param]
    task.test_args = args  # stash args away in task object for future access
    return task

