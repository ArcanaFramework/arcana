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

test_task_names = list(TEST_TASKS)


@pytest.fixture(params=test_task_names)
def pydra_task_details(request):
    func_name = request.param
    return ('arcana2.test_fixtures.tasks.' + func_name,) + tuple(
        TEST_TASKS[func_name][1:])
            

@pytest.fixture(params=test_task_names)
def pydra_task(request):
    task, args = TEST_TASKS[request.param]
    task.test_args = args  # stash args away in task object for future access
    return task
