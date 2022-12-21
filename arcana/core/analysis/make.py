from __future__ import annotations
import typing as ty
import inspect
import itertools
from copy import copy
from operator import attrgetter
import attrs
from ..data.column import DataColumn
from ..data.set import Dataset
from ..utils.misc import (
    PIPELINE_ANNOTATIONS,
    SWICTH_ANNOTATIONS,
    CHECK_ANNOTATIONS,
)
from arcana.core.exceptions import ArcanaDesignError
from .spec import (
    BaseAttr,
    ColumnSpec,
    Parameter,
    BaseMethod,
    SubanalysisSpec,
    PipelineConstructor,
    Switch,
    Check,
    AnalysisSpec,
    _Inherited,
    _MappedFrom,
)


RESERVED_NAMES = ("dataset", "menu", "stack")
RESERVED_NAMES += tuple("_" + n for n in RESERVED_NAMES)


def make(klass: type, space: type) -> type:
    """
    Construct an analysis class and validate all the components fit together

    Parameters
    ----------
    klass : type
        the class that is decorated with the @analysis decorator
    space : type
        a subclass of the DataSpace enum, which defines the types of
        datasets the analysis can be applied to

    Returns
    -------
    type
        the analysis class
    """

    # Initialise lists to hold all the different components of an analysis
    column_specs = []
    pipeline_builders = []
    parameters = []
    switches = []
    checks = []
    subanalysis_specs = []

    # Set name and datatype of attributes and Resolve 'Inherited' and 'mapped_from'
    # attributes and create list of columns, parameters and subanalyses
    for name, attr in list(klass.__dict__.items()):
        if name in RESERVED_NAMES:
            raise ArcanaDesignError(
                f"Cannot use reserved name '{name}' for attribute in "
                f"'{klass.__name__}' analysis class"
            )
        # Get attribute type from type annotations
        dtype = klass.__annotations__.get(name)
        if ty.get_origin(dtype) is DataColumn:
            dtype = ty.get_args(dtype)[0]
        # Resolve inherited and mapped attributes
        if isinstance(attr, (_Inherited, _MappedFrom)):
            resolved, resolved_dtype = attr.resolve(name, klass)
            if dtype is None:
                # Copy type annotation across to new class if it isn't present
                dtype = resolved_dtype
            elif dtype is not resolved_dtype and not issubclass(dtype, resolved_dtype):
                raise ArcanaDesignError(
                    f"Cannot change datatype of {name} from {resolved_dtype} to "
                    f"{dtype} as it is not a sub-class"
                )
            attr.resolved_to = resolved
            attr = resolved

        if isinstance(attr, BaseAttr):
            # Save annotated type of column in metadata and convert to Column
            if dtype is None:
                raise ArcanaDesignError(
                    f"Type annotation must be provided for '{name}' {type(attr).__name__}"
                )
            # Set the name and type of the attributes from the class dict and annotaitons
            # respectively. Need to use object setattr to avoid frozen status
            object.__setattr__(attr, "name", name)
            object.__setattr__(attr, "type", dtype)
            if isinstance(attr, ColumnSpec):
                if attr.row_frequency is None:
                    object.__setattr__(
                        attr, "row_frequency", max(space)
                    )  # "Leaf" frequency of the data tree
                column_specs.append(attr)
            elif isinstance(attr, Parameter):
                parameters.append(attr)
            elif isinstance(attr, SubanalysisSpec):
                subanalysis_specs.append(attr)

    # Resolve the mappings from through the subanalysis_specs in a separate loop so the
    # column names can be resolved
    for spec in subanalysis_specs:
        resolved_mappings = []
        for (from_, to) in spec.mappings:
            resolved_mappings.append((from_, to.name))
        # Add in implicit mappings, where a column from the subanalysis has been
        # mapped into the global namespace of the analysis class
        for col_or_param in itertools.chain(column_specs, parameters):
            if col_or_param.mapped_from and col_or_param.mapped_from[0] == spec.name:
                resolved_mappings.append(
                    (col_or_param.mapped_from[1], col_or_param.name)
                )
        object.__setattr__(spec, "mappings", tuple(sorted(resolved_mappings)))

    # Attributes that need to be converted into attrs.fields before the class
    # is attrisfied
    to_convert_to_attrs = column_specs + parameters + subanalysis_specs

    # Loop through all attributes to pick up decorated methods for pipelines, checks
    # and switches
    for attr in klass.__dict__.values():
        try:
            attr_anots = attr.__annotations__
        except AttributeError:
            continue

        if PIPELINE_ANNOTATIONS in attr_anots:
            anots = attr_anots[PIPELINE_ANNOTATIONS]
            outputs = tuple(o.name for o in anots["outputs"])
            input_columns, used_parameters = _get_args_automagically(
                column_specs=column_specs, parameters=parameters, method=attr
            )
            unresolved_condition = anots["condition"]
            if unresolved_condition is not None:
                try:
                    condition = unresolved_condition.name
                except AttributeError:
                    condition = unresolved_condition.resolve(
                        klass,
                        column_specs=column_specs,
                        parameters=parameters,
                    )
            else:
                condition = None
            pipeline_builders.append(
                PipelineConstructor(
                    name=attr.__name__,
                    desc=attr.__doc__,
                    inputs=input_columns,
                    outputs=outputs,
                    parameters=used_parameters,
                    condition=condition,
                    switch=anots["switch"],
                    defined_in=(),
                    method=attr,
                )
            )
        elif SWICTH_ANNOTATIONS in attr_anots:
            input_columns, used_parameters = _get_args_automagically(
                column_specs=column_specs, parameters=parameters, method=attr
            )
            switches.append(
                Switch(
                    name=attr.__name__,
                    desc=__doc__,
                    inputs=input_columns,
                    parameters=used_parameters,
                    defined_in=(),
                    method=attr,
                )
            )
        elif CHECK_ANNOTATIONS in attr_anots:
            anots = attr_anots[CHECK_ANNOTATIONS]
            column_name = anots["column"].name
            input_columns, used_parameters = _get_args_automagically(
                column_specs=column_specs, parameters=parameters, method=attr
            )
            checks.append(
                Check(
                    name=attr.__name__,
                    column=column_name,
                    desc=attr.__doc__,
                    inputs=input_columns,
                    parameters=used_parameters,
                    salience=anots["salience"],
                    defined_in=(),
                    method=attr,
                )
            )

    to_set_defined_in = to_convert_to_attrs + pipeline_builders + switches + checks

    # Combine with specs from base classes
    for base in klass.__mro__[1:]:
        if not hasattr(base, "__spec__"):
            continue  # skip classes that aren't decorated analyses
        if base.__spec__.space is not space:  # TODO: permit "super spaces"
            raise ValueError(
                "Cannot redefine the space that an analysis operates on from "
                f"{base.__spec__.space} to {space}"
            )
        # Append column specs, parameters and subanalyses that were inherited from base
        # classes
        for lst, base_lst in (
            (column_specs, base.__spec__.column_specs),
            (parameters, base.__spec__.parameters),
            (subanalysis_specs, base.__spec__.subanalysis_specs),
        ):
            if not_inherited_explicitly := [
                x.name
                for x in lst
                if x.name in (b.name for b in base_lst) and not x.inherited
            ]:
                raise ArcanaDesignError(
                    f"{not_inherited_explicitly} attributes in {klass} implicitly override "
                    f"the corresponding attributes in {base} (i.e. without using the "
                    "inherit() function)"
                )
            lst.extend(b for b in base_lst if b.name not in (x.name for x in lst))
        # Append methods to those that that were inherited from base classes
        for lst, base_lst in (
            (pipeline_builders, base.__spec__.pipeline_builders),
            (switches, base.__spec__.switches),
            (checks, base.__spec__.checks),
        ):
            for base_method in base_lst:
                try:
                    method = next(m for m in lst if m.name == base_method.name)
                except StopIteration:
                    continue
                # Copy across defined attribute
                object.__setattr__(method, "defined_in", base_method.defined_in)
                # Check pipeline builders to see that they don't remove outputs
                if isinstance(method, PipelineConstructor):
                    if missing_outputs := [
                        o for o in base_method.outputs if o not in method.outputs
                    ]:
                        raise ArcanaDesignError(
                            f"{missing_outputs} outputs are missing from '{method.name}' "
                            "pipeline builder, which were defined by the overridden method "
                            "in {base}. Overriding methods can only add new outputs, not "
                            "remove existing ones"
                        )
            lst.extend(b for b in base_lst if b.name not in (x.name for x in lst))

    analysis_spec = AnalysisSpec(
        space=space,
        column_specs=tuple(sorted(column_specs, key=attrgetter("name"))),
        pipeline_builders=tuple(sorted(pipeline_builders, key=attrgetter("name"))),
        parameters=tuple(sorted(parameters, key=attrgetter("name"))),
        switches=tuple(sorted(switches, key=attrgetter("name"))),
        checks=tuple(sorted(checks, key=attrgetter("name"))),
        subanalysis_specs=tuple(sorted(subanalysis_specs, key=attrgetter("name"))),
    )

    # Now that we have saved the attributes in lists to be
    for attr in to_convert_to_attrs:
        attrs_field = attr.to_attrs_field()
        setattr(klass, attr.name, attrs_field)
        klass.__annotations__[attr.name] = dtype

    # Ensure that the class has it's own annotaitons dict so we can modify it without
    # messing up other classes
    klass.__annotations__ = copy(klass.__annotations__)
    klass._dataset = attrs.field(default=None, validator=_dataset_validator)
    klass.__annotations__["_dataset"] = Dataset

    # Set built-in methods
    klass.menu = MenuDescriptor()
    klass.stack = StackDescriptor()
    klass.__attrs_post_init__ = _analysis_post_init

    # Add the analysis spec to the __spec__ attribute
    klass.__spec__ = analysis_spec
    klass.__annotations__["__spec__"] = AnalysisSpec

    # Create class using attrs package, will create attributes for all columns and
    # parameters
    attrs_klass = attrs.define(auto_attribs=False)(klass)

    # Set the class that newly attribute attributes were defined in
    for attr in to_set_defined_in:
        # Register the attribute/method as being defined in this class if it has
        # been either added or created
        if isinstance(attr, BaseMethod) or len(attr.modified) == len(attr.defined_in):
            object.__setattr__(attr, "defined_in", attr.defined_in + (attrs_klass,))

    return attrs_klass


def _analysis_post_init(self):
    """Set up links to dataset columns and initialise subanalysis classes in after the
    attrs init"""
    for spec in self.__spec__.column_specs:
        column = getattr(self, spec.name)
        if isinstance(column, str):
            column = self._dataset[column]
            setattr(self, spec.name, column)
        elif column not in (None, attrs.NOTHING) and not isinstance(column, DataColumn):
            raise ValueError(
                f"Value passed to '{spec.name}' ({column}) should either be a "
                "data column or the name of a column"
            )
    for spec in self.__spec__.subanalysis_specs:
        subanalysis = getattr(self, spec.name)
        if isinstance(subanalysis, dict):
            kwargs = {m[0]: attrs.NOTHING for m in spec.mappings}
            if overwritten_mapped := list(set(kwargs) & set(subanalysis)):
                raise ValueError(
                    f"{overwritten_mapped} attributes cannot be set explicitly in '"
                    f"{spec.name}' sub-analysis as they are mapped from the enclosing "
                    "analysis"
                )
            kwargs.update(subanalysis)
            subanalysis = Subanalysis(
                spec=spec,
                analysis=spec.type(dataset=self._dataset, **kwargs),
                parent=self,
            )
            setattr(self, spec.name, subanalysis)
        elif not isinstance(subanalysis, Subanalysis):
            raise ValueError(
                f"Value passed to '{spec.name}' ({subanalysis}) should either be a "
                "subanalysis or a dict containing keyword args to initialise a "
                "subanalysis"
            )


def _get_args_automagically(column_specs, parameters, method, index_start=2):
    """Automagically determine inputs to pipeline or switched by matching
    a methods argument names with columns and parameters of the class

    Parameters
    ----------
    column_specs : list[ColumnSpec]
        the column specs to match the inputs against
    parameters : list[Parameter]
        the parameters to match the inputs against
    method : bound-method
        the method to automagically determine the inputs for
    index_start : int
        the argument index to start from (i.e. can be used to skip the workflow
        arg passed to pipelines classes)

    Returns
    -------
    list[str]
        the names of the input columns to automagically provide to the method
    list[str]
        the names of the parameters to automagically provide to the method
    """
    inputs = []
    used_parameters = []
    column_names = [c.name for c in column_specs]
    param_names = [p.name for p in parameters]
    signature = inspect.signature(method)
    for arg in list(signature.parameters)[
        index_start:
    ]:  # First arg is self and second is the workflow object to add to
        required_type = method.__annotations__.get(arg)
        if arg in column_names:
            column_spec = next(c for c in column_specs if c.name == arg)
            if required_type is not None and required_type is not column_spec.type:
                # Check to see whether conversion is possible
                required_type.find_converter(column_spec.type)
            inputs.append(arg)
        elif arg in param_names:
            used_parameters.append(arg)
        else:
            raise ArcanaDesignError(
                f"Unrecognised argument '{arg}'. If it is from a base class, "
                "make sure that it is explicitly inherited using the `Inherited` "
                "function."
            )
    return tuple(inputs), tuple(used_parameters)


def _dataset_validator(self, _, val):
    if not val:
        raise ValueError(f"A dataset must be provided when initialising {self} ")


class MenuDescriptor:
    def __get__(self, ins, typ):
        if ins is None:
            raise NotImplementedError("Class-based menu calls are not implemented")
        else:
            raise NotImplementedError("Instance-based menu calls are not implemented")


class StackDescriptor:
    def __get__(self, ins, typ):
        if ins is None:
            raise NotImplementedError("Class-based stack calls are not implemented")
        else:
            raise NotImplementedError("Instance-based stack calls are not implemented")


@attrs.define
class Subanalysis:
    """Wrapper around the actual analysis class of the subanalysis, which performs the
    mapping of attributes"""

    _spec: SubanalysisSpec
    _analysis: ty.Any  # initialised analysis class for the subanalysis
    _parent: ty.Any  # reference back to the parent analysis class

    def __repr__(self):
        return f"{type(self).__name__}(_name={self._spec.name}, type={type(self._analysis)})"

    def __getattr__(self, name: str) -> ty.Any:
        try:
            return object.__getattr__(self, name)
        except AttributeError:
            pass
        try:
            mapped_name = self._spec.mapping(name)
        except KeyError:
            value = getattr(self._analysis, name)
        else:
            value = getattr(self._parent, mapped_name)
        return value

    def __setattr__(self, name, value):
        if name in ("_spec", "_analysis", "_parent"):
            object.__setattr__(self, name, value)
        else:
            try:
                mapped_name = self._spec.mapping(name)
            except KeyError:
                pass
            else:
                raise AttributeError(
                    f"Cannot set value of attribute '{name}' in '{self._spec.name}' "
                    f"sub-analysis as it is mapped to '{mapped_name}' in the parent "
                    f"analysis {self._parent}"
                )
            setattr(self._analysis, name, value)

    def __attrs_post_init__(self):
        """Ensure any nested subanalyses reference this subanalysis as the parent
        instead of the wrapped analysis object in order to link up chains of name
        mappings
        """
        for spec in self._analysis.__spec__.subanalysis_specs:
            subanalysis = getattr(self._analysis, spec.name)
            subanalysis._parent = self
