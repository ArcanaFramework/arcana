from copy import deepcopy
import json
import re
from pprint import pformat
import datetime
from deepdiff import DeepDiff
from arcana.exceptions import ArcanaError, ArcanaUsageError


class DataProvenance():
    """
    A representation of the information required to describe the provenance of
    analysis sinks. Provenances the provenance information relevant to a
    specific session, i.e. the general configuration of the pipeline and file
    checksums|field values of the pipeline inputs used to derive the outputs in
    a given session (or timepoint, subject, analysis summary). It also provenances
    the checksums|values of the outputs in order to detect if they have been
    altered outside of Arcana's management (e.g. manual QC/correction)

    Parameters
    ----------
    dct : ty.Dict[str, Any]
        A dictionary containing the provenance record
    """

    PROV_VERSION_KEY = '__prov_version__'
    PROV_VERSION = '1.0'
    DATETIME = 'datetime'

    def __init__(self, dct):
        self.dct = deepcopy(dct)
        if self.DATETIME not in self.dct:
            self.dct[self.DATETIME] = datetime.now().isoformat()
        if self.PROV_VERSION_KEY not in self.dct:
            self.dct[self.PROV_VERSION_KEY] = self.PROV_VERSION

    def __repr__(self):
        return repr(self.dct)

    def __eq__(self, other):
        return self.dct == other.dct

    def __getitem__(self, key):
        return self.dct[key]

    def __setitem__(self, key, value):
        self.dct[key] = value

    def items(self):
        return self.dct.items()

    @property
    def datetime(self):
        return self.dct[self.DATETIME]

    @property
    def version(self):
        return self.dct[self.PROV_VERSION_KEY]

    def save(self, file_path):
        """
        Saves the provenance object to a JSON file, optionally including
        checksums for inputs and outputs (which are initially produced mid-
        run) to insert during the write

        Parameters
        ----------
        name_path : str
            Path to save the generated JSON file
        inputs : ty.Dict[str, str | ty.List[str] | ty.List[ty.List[str]]] | None
            Checksums of all pipeline inputs used by the pipeline. For inputs
            of matching frequency to the output sink associated with the
            provenance object, the values of the dictionary will be single
            checksums. If the output is of lower frequency they will be lists
            of checksums or in the case of 'per_session' inputs to 'per_dataset'
            outputs, lists of lists of checksum. They need to be provided here
            if the provenance object was initialised without checksums
        outputs : ty.Dict[str, str] | None
            Checksums of all pipeline outputs. They need to be provided here
            if the provenance object was initialised without checksums
        """
        with open(file_path, 'w') as f:
            try:
                json.dump(self.dct, f, sort_keys=True, indent=2)
            except TypeError:
                raise ArcanaError(
                    "Could not serialise provenance provenance dictionary:\n{}"
                    .format(pformat(self.dct)))

    @classmethod
    def load(cls, file_path, ignore_missing=False):
        """
        Loads a saved provenance object from a JSON file

        Parameters
        ----------
        file_path : str
            The name_path to a local file containing the provenance JSON
        ignore_missing : bool
            Return None if the file doesn't exist

        Returns
        -------
        provenance : Provenance
            The loaded provenance provenance
        """
        try:
            with open(file_path) as f:
                dct = json.load(f)
        except FileNotFoundError:
            if ignore_missing:
                return None
            raise
        else:
            return DataProvenance(dct)

    def mismatches(self, other, include=None, exclude=None):
        """
        Compares information stored within provenance objects with the
        exception of version information to see if they match. Matches are
        constrained to the name_paths passed to the 'include' kwarg, with the
        exception of sub-name_paths passed to the 'exclude' kwarg

        Parameters
        ----------
        other : Provenance
            The provenance object to compare against
        include : ty.List[ty.List[str]] | None
            Paths in the provenance to include in the match. If None all are
            incluced
        exclude : ty.List[ty.List[str]] | None
            Paths in the provenance to exclude from the match. In None all are
            excluded
        """
        if include is not None:
            include_res = [self._gen_prov_path_regex(p) for p in include]
        if exclude is not None:
            exclude_res = [self._gen_prov_path_regex(p) for p in exclude]
        diff = DeepDiff(self._prov, other._prov, ignore_order=True)
        # Create regular expresssions for the include and exclude name_paths in
        # the format that deepdiff uses for nested dictionary/lists

        def include_change(change):
            if include is None:
                included = True
            else:
                included = any(rx.match(change) for rx in include_res)
            if included and exclude is not None:
                included = not any(rx.match(change) for rx in exclude_res)
            return included

        filtered_diff = {}
        for change_type, changes in diff.items():
            if isinstance(changes, dict):
                filtered = dict((k, v) for k, v in changes.items()
                                if include_change(k))
            else:
                filtered = [c for c in changes if include_change(c)]
            if filtered:
                filtered_diff[change_type] = filtered
        return filtered_diff

    @classmethod
    def _gen_prov_path_regex(self, file_path):
        if isinstance(file_path, str):
            if file_path.startswith('/'):
                file_path = file_path[1:]
            regex = re.compile(r"root\['{}'\].*"
                               .format(r"'\]\['".join(file_path.split('/'))))
        elif not isinstance(file_path, re.Pattern):
            raise ArcanaUsageError(
                "Provenance in/exclude name_paths can either be name_path "
                "strings or regexes, not '{}'".format(file_path))
        return regex
