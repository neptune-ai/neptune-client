# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: neptune_pb/ingest/v1/common.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n!neptune_pb/ingest/v1/common.proto\x12\x11neptune.ingest.v1\x1a\x1fgoogle/protobuf/timestamp.proto\"$\n\x04Step\x12\r\n\x05whole\x18\x01 \x01(\x04\x12\r\n\x05micro\x18\x02 \x01(\x04\"a\n\tForkPoint\x12\x16\n\x0eparent_project\x18\x01 \x01(\t\x12\x15\n\rparent_run_id\x18\x02 \x01(\t\x12%\n\x04step\x18\x04 \x01(\x0b\x32\x17.neptune.ingest.v1.Step\"\x1b\n\tStringSet\x12\x0e\n\x06values\x18\x01 \x03(\t\"\xbb\x01\n\x05Value\x12\x11\n\x07\x66loat64\x18\x01 \x01(\x01H\x00\x12\x0f\n\x05int64\x18\x03 \x01(\x03H\x00\x12\x0e\n\x04\x62ool\x18\x05 \x01(\x08H\x00\x12\x10\n\x06string\x18\x06 \x01(\tH\x00\x12/\n\ttimestamp\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x12\x32\n\nstring_set\x18\x0c \x01(\x0b\x32\x1c.neptune.ingest.v1.StringSetH\x00\x42\x07\n\x05value\"\xa2\x01\n\x0fModifyStringSet\x12>\n\x06values\x18\x01 \x03(\x0b\x32..neptune.ingest.v1.ModifyStringSet.ValuesEntry\x1aO\n\x0bValuesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12/\n\x05value\x18\x02 \x01(\x0e\x32 .neptune.ingest.v1.SET_OPERATION:\x02\x38\x01\"I\n\tModifySet\x12\x34\n\x06string\x18\x01 \x01(\x0b\x32\".neptune.ingest.v1.ModifyStringSetH\x00\x42\x06\n\x04type\"\xef\x01\n\x03Run\x12\x13\n\x06run_id\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x1a\n\rexperiment_id\x18\x05 \x01(\tH\x01\x88\x01\x01\x12\x30\n\nfork_point\x18\x02 \x01(\x0b\x32\x1c.neptune.ingest.v1.ForkPoint\x12\x13\n\x06\x66\x61mily\x18\x04 \x01(\tH\x02\x88\x01\x01\x12\x36\n\rcreation_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x03\x88\x01\x01\x42\t\n\x07_run_idB\x10\n\x0e_experiment_idB\t\n\x07_familyB\x10\n\x0e_creation_time\"\x9b\x04\n\x11UpdateRunSnapshot\x12%\n\x04step\x18\x01 \x01(\x0b\x32\x17.neptune.ingest.v1.Step\x12-\n\ttimestamp\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12@\n\x06\x61ssign\x18\x04 \x03(\x0b\x32\x30.neptune.ingest.v1.UpdateRunSnapshot.AssignEntry\x12I\n\x0bmodify_sets\x18\x05 \x03(\x0b\x32\x34.neptune.ingest.v1.UpdateRunSnapshot.ModifySetsEntry\x12@\n\x06\x61ppend\x18\x08 \x03(\x0b\x32\x30.neptune.ingest.v1.UpdateRunSnapshot.AppendEntry\x1aG\n\x0b\x41ssignEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.neptune.ingest.v1.Value:\x02\x38\x01\x1aO\n\x0fModifySetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12+\n\x05value\x18\x02 \x01(\x0b\x32\x1c.neptune.ingest.v1.ModifySet:\x02\x38\x01\x1aG\n\x0b\x41ppendEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.neptune.ingest.v1.Value:\x02\x38\x01*.\n\rSET_OPERATION\x12\x08\n\x04NOOP\x10\x00\x12\x07\n\x03\x41\x44\x44\x10\x01\x12\n\n\x06REMOVE\x10\x02\x42\x35\n$ml.neptune.leaderboard.api.ingest.v1B\x0b\x43ommonProtoP\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.ingest.v1.common_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n$ml.neptune.leaderboard.api.ingest.v1B\013CommonProtoP\001'
  _MODIFYSTRINGSET_VALUESENTRY._options = None
  _MODIFYSTRINGSET_VALUESENTRY._serialized_options = b'8\001'
  _UPDATERUNSNAPSHOT_ASSIGNENTRY._options = None
  _UPDATERUNSNAPSHOT_ASSIGNENTRY._serialized_options = b'8\001'
  _UPDATERUNSNAPSHOT_MODIFYSETSENTRY._options = None
  _UPDATERUNSNAPSHOT_MODIFYSETSENTRY._serialized_options = b'8\001'
  _UPDATERUNSNAPSHOT_APPENDENTRY._options = None
  _UPDATERUNSNAPSHOT_APPENDENTRY._serialized_options = b'8\001'
  _globals['_SET_OPERATION']._serialized_start=1469
  _globals['_SET_OPERATION']._serialized_end=1515
  _globals['_STEP']._serialized_start=89
  _globals['_STEP']._serialized_end=125
  _globals['_FORKPOINT']._serialized_start=127
  _globals['_FORKPOINT']._serialized_end=224
  _globals['_STRINGSET']._serialized_start=226
  _globals['_STRINGSET']._serialized_end=253
  _globals['_VALUE']._serialized_start=256
  _globals['_VALUE']._serialized_end=443
  _globals['_MODIFYSTRINGSET']._serialized_start=446
  _globals['_MODIFYSTRINGSET']._serialized_end=608
  _globals['_MODIFYSTRINGSET_VALUESENTRY']._serialized_start=529
  _globals['_MODIFYSTRINGSET_VALUESENTRY']._serialized_end=608
  _globals['_MODIFYSET']._serialized_start=610
  _globals['_MODIFYSET']._serialized_end=683
  _globals['_RUN']._serialized_start=686
  _globals['_RUN']._serialized_end=925
  _globals['_UPDATERUNSNAPSHOT']._serialized_start=928
  _globals['_UPDATERUNSNAPSHOT']._serialized_end=1467
  _globals['_UPDATERUNSNAPSHOT_ASSIGNENTRY']._serialized_start=1242
  _globals['_UPDATERUNSNAPSHOT_ASSIGNENTRY']._serialized_end=1313
  _globals['_UPDATERUNSNAPSHOT_MODIFYSETSENTRY']._serialized_start=1315
  _globals['_UPDATERUNSNAPSHOT_MODIFYSETSENTRY']._serialized_end=1394
  _globals['_UPDATERUNSNAPSHOT_APPENDENTRY']._serialized_start=1396
  _globals['_UPDATERUNSNAPSHOT_APPENDENTRY']._serialized_end=1467
# @@protoc_insertion_point(module_scope)
