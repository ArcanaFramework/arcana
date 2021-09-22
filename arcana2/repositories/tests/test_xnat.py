import os
import os.path
from datetime import datetime
import operator as op
from itertools import product
from functools import reduce
from copy import copy
import pytest
from arcana2.repositories.file_system import FileSystem
from arcana2.dimensions.clinical import Clinical



@pytest.fixture
def dataset(work_dir, xnat_repo, request):
    "Creates a dataset from parameters in TEST_SETS"
    name, dim_lengths, scans, id_inference = request.param

    # Create a new project on the test XNAT
    proj_name = name + datetime.strftime(datetime.now(), '%Y%m%d%H%M%S%f')
    xnat_repo.new_dataset(proj_name)
    proj_work_dir = work_dir / proj_name

    os.mkdir(proj_work_dir)

    with xnat_repo:
        xproject = xnat_repo.login.projects[proj_name]

        xclasses = xnat_repo.login.classes

        for id_tple in product(*(list(range(d)) for d in dim_lengths)):
            ids = dict(zip(Clinical.basis(), id_tple))

            # Create subject
            subject_label = ''.join(
                f'{b}{ids[b]}' for b in Clinical.subject.nonzero_basis())
            xsubject = xclasses.SubjectData(label=subject_label,
                                            parent=xproject)

            # Create session
            session_label = ''.join(
                f'{b}{ids[b]}' for b in Clinical.session.nonzero_basis())
            xsession = xclasses.MrSessionData(label=session_label,
                                              parent=xsubject)

            for i, (sname, resources) in enumerate(scans, start=1):
                spath = proj_work_dir / sname
                os.mkdir(spath)

                # Create scan
                xscan = xclasses.MrScanData(id=i, type=sname, parent=xsession)

                for rname, fnames in resources:
                    rpath = spath / rname
                    os.mkdir(rpath)

                    # Create the resource
                    xresource = xscan.create_resource(rname)

                    # Create the dummy files
                    for fname in fnames:
                        fpath = fname
                        # Make double
                        if fname.startswith('doubledir'):
                            os.mkdir(rpath / fpath)
                            fname = 'dir'
                            fpath /= fname
                        if fname.startswith('dir'):
                            os.mkdir(rpath / fpath)
                            fname = 'test.txt'
                            fpath /= fname
                        with open(fpath, 'w') as f:
                            f.write(f'test {fname}')
                        xresource.upload(str(fpath), str(rpath / fpath))

    dataset = xnat_repo.dataset(proj_name, id_inference=id_inference)
    dataset.dim_lengths = dim_lengths
    dataset.scans = scans
    yield dataset
    dataset.delete()


TEST_SETS = [
    (
        'basic',  # dataset name
        [1, 1, 3],  # number of timepoints, groups and members respectively
        [['scan1', [ # scan type (ID is index)
            ('TEXT', ['file.txt'])]], # resource name and files within them
         ['scan2', [
             ('NIFTIX_GZ', ['file.nii.gz'])]],
         ['scan3', [
             ('FREESURFER', ['doubledir', 'dir', 'file.dat'])]],
         ['scan4', [
             ('DICOM', ['file1.dcm', 'file2.dcm', 'file3.dcm']),
             ('NIFTI', ['file1.nii.gz']),
             ('BIDS', ['file1.json']),
             ('SNAPSHOT', ['file1.png'])]]],  
        {}  # id_inference dict
    ),]


@pytest.mark.parametrize('dataset', TEST_SETS, indirect=True)
def test_construct_tree(dataset):
    for freq in Clinical:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")
