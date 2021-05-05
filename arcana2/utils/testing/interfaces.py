from __future__ import print_function
from past.builtins import basestring
import os.path as op
import operator
from functools import reduce
from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterface, isdefined)
from arcana2.exceptions import ArcanaUsageError


class TestMathInputSpec(TraitedSpec):

    x = traits.Either(traits.Float(), traits.File(exists=True),
                      traits.List(traits.Float),
                      traits.List(traits.File(exists=True)),
                      desc='first arg')
    y = traits.Either(traits.Float(), traits.File(exists=True),
                      mandatory=False, desc='second arg')
    op = traits.Str(mandatory=True, desc='operation')

    z = traits.File(genfile=True, mandatory=False,
                    desc="Name for output file")

    as_file = traits.Bool(False, desc="Whether to write as a file",
                          usedefault=True)


class TestMathOutputSpec(TraitedSpec):

    z = traits.Either(traits.Float(), traits.File(exists=True),
                      'output')


class TestMath(BaseInterface):
    """
    A basic interface to test out the pipeline infrastructure
    """

    input_spec = TestMathInputSpec
    output_spec = TestMathOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        x = self.inputs.x
        y = self.inputs.y
        if isinstance(x, basestring):
            x = self._load_file(x)
        if isinstance(y, basestring):
            y = self._load_file(y)
        oper = getattr(operator, self.inputs.op)
        if isdefined(y):
            z = oper(x, y)
        elif isinstance(x, list):
            if x:
                if isinstance(x[0], basestring):
                    x = [self._load_file(u) for u in x]
            else:
                raise ArcanaUsageError("Cannot provide empty list to 'x'")
            z = reduce(oper, x)
        else:
            raise Exception(
                "If 'y' is not provided then x needs to be list")
        outputs = self.output_spec().get()
        if self.inputs.as_file:
            z_path = op.abspath(self._gen_z_fname())
            with open(z_path, 'w') as f:
                f.write(str(z))
            outputs['z'] = z_path
        else:
            outputs['z'] = z
        return outputs

    def _gen_filename(self, name):
        if name == 'z':
            fname = self._gen_z_fname()
        else:
            assert False
        return fname

    def _gen_z_fname(self):
        if isdefined(self.inputs.z):
            fname = self.inputs.z
        else:
            fname = 'z.txt'
        return fname

    @classmethod
    def _load_file(self, path):
        with open(path) as f:
            try:
                return float(f.read())
            except:
                raise
