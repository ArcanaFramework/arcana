from __future__ import annotations
import typing as ty
import builtins
import attrs
from arcana.core.utils.serialize import (
    ObjectConverter,
    ClassResolver,
)
from arcana.core.analysis.pipeline import (
    PipelineField,
)
from arcana.core.data.row import DataRow
from arcana.core.data.type.base import DataType
from arcana.core.exceptions import ArcanaFormatConversionError
from arcana.core.data.space import DataSpace
from arcana.core.utils.misc import add_exc_note


@attrs.define
class DefaultColumn:
    """
    Values to set the default column of a command input/output

    Parameters
    ----------
    datatype : str
        the type the data items will be stored in (e.g. file-format)
    row_frequency : DataSpace
        the "row-frequency" of the input column to be added
    path : str
        path to where the data will be placed in the repository
    """

    datatype: type = attrs.field(
        default=None,
        converter=ClassResolver(
            DataType,
            allow_none=True,
            alternative_types=[DataRow],
        ),
    )
    row_frequency: DataSpace = None
    path: str = None


@attrs.define(kw_only=True)
class CommandField(PipelineField):
    """An input/output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input, arcana.dirtree.data.File by default
    help_string : str
        short description of the field to be displayed in the UI
    configuration : dict[str, Any] or bool
        Arguments that should be passed onto the the command configuration dict in
        special the ``inputs``, ``outputs`` or ``parameters`` input fields for the given
        field alongside the name and datatype. Should be set to True if just the name
        and dataset
    """

    help_string: str
    configuration: ty.Union[dict[str, ty.Any], bool] = attrs.field(factory=dict)

    @property
    def config_dict(self):
        """Returns a dictionary to be passed to the task/workflow in order to configure
        it to receive input/output

        Parameters
        ----------
        configuration : _type_
            _description_
        list_name : _type_
            _description_
        """
        if self.configuration:
            config = {
                "name": self.name,
                "datatype": self.datatype,
            }
            if isinstance(self.configuration, dict):  # Otherwise just True
                config.update(self.configuration)
        else:
            config = {}
        return config


@attrs.define(kw_only=True)
class CommandInput(CommandField):
    """Defines an input or output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help_string : str
        description of the input/output field
    configuration : dict
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `inputs` input field to the task/workflow
    default_columm: DefaultColumn, optional
        the values to use to configure a default column if the name doesn't match an
        existing column
    """

    default_column: DefaultColumn = attrs.field(
        converter=ObjectConverter(
            DefaultColumn, allow_none=True, default_if_none=DefaultColumn
        ),
        default=None,
    )

    @default_column.validator
    def default_column_validator(self, _, default_column: DefaultColumn):
        if (
            default_column.datatype is not None
            and self.datatype is not default_column.datatype
            and not isinstance(
                self.datatype, str
            )  # if has fallen back to string in non-build envs
        ):
            try:
                self.datatype.find_converter(default_column.datatype)
            except ArcanaFormatConversionError as e:
                add_exc_note(
                    e,
                    f"required to convert from the default column to the '{self.name}' input",
                )
                raise

    def __attrs_post_init__(self):
        if self.default_column.datatype is None:
            self.default_column.datatype = self.datatype


@attrs.define(kw_only=True)
class CommandOutput(CommandField):
    """Defines an input or output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help_string : str
        description of the input/output field
    configuration : dict
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `outputs` input field to the task/workflow
    default_columm: DefaultColumn, optional
        the values to use to configure a default column if the name doesn't match an
        existing column
    """

    default_column: DefaultColumn = attrs.field(
        converter=ObjectConverter(
            DefaultColumn, allow_none=True, default_if_none=DefaultColumn
        ),
        default=None,
    )

    @default_column.validator
    def default_column_validator(self, _, default_column: DefaultColumn):
        if (
            default_column.datatype is not None
            and self.datatype is not default_column.datatype
        ):
            try:
                default_column.datatype.find_converter(self.datatype)
            except ArcanaFormatConversionError as e:
                add_exc_note(
                    e,
                    f"required to convert to the default column from the '{self.name}' output",
                )
                raise

    def __attrs_post_init__(self):
        if self.default_column.datatype is None:
            self.default_column.datatype = self.datatype


@attrs.define(kw_only=True)
class CommandParameter(CommandField):
    """Defines a fixed parameter of the task/workflow/analysis to be exposed in the UI

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help_string : str
        description of the input/output field
    configuration : dict[str, Any]
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `parameters` input field to the task/workflow
    required : bool
        whether the parameter is required or not
    default : Any
        the default value for the parameter, must be able to be
    """

    datatype: ty.Union[int, float, bool, str] = attrs.field(
        converter=lambda x: getattr(builtins, x) if isinstance(x, str) else x
    )
    required: bool = False
    default: ty.Any = None
