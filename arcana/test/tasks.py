import shutil
from pathlib import Path
import attr
from pydra import mark, Workflow
from arcana.core.data.row import DataRow



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


@mark.task
@mark.annotate({
    'return': {'out_file': Path}})
def reverse(in_file: Path, out_file: Path=None) -> Path:
    """Reverses the contents of a file and outputs it to another file

    Parameters
    ----------
    in_file : Path
        A text file
    out_file : Path
       The path to write the output file to 

    Returns
    -------
    Path
        A text file with reversed contents to the original
    """
    if out_file is None:
        out_file = Path('out_file.txt').absolute()
    with open(in_file) as f:
        contents = f.read()
    with open(out_file, 'w') as f:
        f.write(contents[::-1])
    return out_file


def concatenate_reverse(name='concatenate_reverse', **kwargs):
    """A simple workflow that has the same signature as concatenate, but
    concatenates reversed contents of the input files instead

    Parameters
    ----------
    name : str
        name of the workflow to be created
    **kwargs
        keyword arguments passed through to the workflow init, can be any of
        the workflow's input spec, i.e. ['in_file1', 'in_file2', 'duplicates']

    Returns
    -------
    Workflow
        the workflow that 
    """
    wf = Workflow(name=name, input_spec=['in_file1', 'in_file2', 'duplicates'],
                  **kwargs)

    wf.add(reverse(
        name='reverse1',
        in_file=wf.lzin.in_file1))

    wf.add(reverse(
        name='reverse2',
        in_file=wf.lzin.in_file2))

    wf.add(concatenate(
        name='concatenate',
        in_file1=wf.reverse1.lzout.out_file,
        in_file2=wf.reverse2.lzout.out_file,
        duplicates=wf.lzin.duplicates))

    wf.set_output([('out_file', wf.concatenate.lzout.out_file)])

    return wf


@mark.task
def plus_10_to_filenumbers(filenumber_row: DataRow) -> None:
    """Renames all the files it finds in the data row (unresolved), assumes their
    stems are convertable to an integer, and renames the file so this integer
    is +10. Used in the test_run_pipeline_on_row_cli test.

    Parameters
    ----------
    row : DataRow
        the data row to modify
    """
    for item in filenumber_row.unresolved:
        fs_path = item.file_paths[0]
        filenumber = int(fs_path.stem)
        new_path = (fs_path.parent
                    / str(filenumber + 10)).with_suffix(fs_path.suffix)
        shutil.move(fs_path, new_path)
