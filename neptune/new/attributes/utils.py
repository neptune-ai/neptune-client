from neptune.new.attributes import (
    Boolean, Datetime, File, FileSeries, FileSet, Float, FloatSeries, GitRef, Integer, RunState, String, StringSeries,
    StringSet,
)
from neptune.new.internal.backends.api_model import AttributeType

attribute_type_to_atom = {
    AttributeType.FLOAT: Float,
    AttributeType.INT: Integer,
    AttributeType.BOOL: Boolean,
    AttributeType.STRING: String,
    AttributeType.DATETIME: Datetime,
    AttributeType.FILE: File,
    AttributeType.FILE_SET: FileSet,
    AttributeType.FLOAT_SERIES: FloatSeries,
    AttributeType.STRING_SERIES: StringSeries,
    AttributeType.IMAGE_SERIES: FileSeries,
    AttributeType.STRING_SET: StringSet,
    AttributeType.GIT_REF: GitRef,
    AttributeType.RUN_STATE: RunState,
    AttributeType.NOTEBOOK_REF: None,
}
