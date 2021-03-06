# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: sync.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='sync.proto',
  package='distdb.sync',
  serialized_pb=_b('\n\nsync.proto\x12\x0b\x64istdb.sync\"\x15\n\x05Hello\x12\x0c\n\x04name\x18\x01 \x02(\t\"@\n\x05Setup\x12\x19\n\x11last_serial_in_db\x18\x01 \x02(\x04\x12\x1c\n\x14last_serial_received\x18\x02 \x02(\x04\"B\n\x06Update\x12\x0b\n\x03obj\x18\x01 \x02(\t\x12\x0b\n\x03key\x18\x02 \x02(\t\x12\x0e\n\x06tstamp\x18\x03 \x02(\x03\x12\x0e\n\x06values\x18\x04 \x03(\t\"\x1c\n\nCheckpoint\x12\x0e\n\x06serial\x18\x01 \x02(\x03\"\xa0\x01\n\x06Stream\x12!\n\x05hello\x18\x01 \x02(\x0b\x32\x12.distdb.sync.Hello\x12!\n\x05setup\x18\x02 \x02(\x0b\x32\x12.distdb.sync.Setup\x12#\n\x06update\x18\x03 \x03(\x0b\x32\x13.distdb.sync.Update\x12+\n\ncheckpoint\x18\x04 \x03(\x0b\x32\x17.distdb.sync.Checkpoint')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_HELLO = _descriptor.Descriptor(
  name='Hello',
  full_name='distdb.sync.Hello',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='distdb.sync.Hello.name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=27,
  serialized_end=48,
)


_SETUP = _descriptor.Descriptor(
  name='Setup',
  full_name='distdb.sync.Setup',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='last_serial_in_db', full_name='distdb.sync.Setup.last_serial_in_db', index=0,
      number=1, type=4, cpp_type=4, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='last_serial_received', full_name='distdb.sync.Setup.last_serial_received', index=1,
      number=2, type=4, cpp_type=4, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=50,
  serialized_end=114,
)


_UPDATE = _descriptor.Descriptor(
  name='Update',
  full_name='distdb.sync.Update',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='obj', full_name='distdb.sync.Update.obj', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='key', full_name='distdb.sync.Update.key', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='tstamp', full_name='distdb.sync.Update.tstamp', index=2,
      number=3, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='values', full_name='distdb.sync.Update.values', index=3,
      number=4, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=116,
  serialized_end=182,
)


_CHECKPOINT = _descriptor.Descriptor(
  name='Checkpoint',
  full_name='distdb.sync.Checkpoint',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='serial', full_name='distdb.sync.Checkpoint.serial', index=0,
      number=1, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=184,
  serialized_end=212,
)


_STREAM = _descriptor.Descriptor(
  name='Stream',
  full_name='distdb.sync.Stream',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hello', full_name='distdb.sync.Stream.hello', index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='setup', full_name='distdb.sync.Stream.setup', index=1,
      number=2, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='update', full_name='distdb.sync.Stream.update', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='checkpoint', full_name='distdb.sync.Stream.checkpoint', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=215,
  serialized_end=375,
)

_STREAM.fields_by_name['hello'].message_type = _HELLO
_STREAM.fields_by_name['setup'].message_type = _SETUP
_STREAM.fields_by_name['update'].message_type = _UPDATE
_STREAM.fields_by_name['checkpoint'].message_type = _CHECKPOINT
DESCRIPTOR.message_types_by_name['Hello'] = _HELLO
DESCRIPTOR.message_types_by_name['Setup'] = _SETUP
DESCRIPTOR.message_types_by_name['Update'] = _UPDATE
DESCRIPTOR.message_types_by_name['Checkpoint'] = _CHECKPOINT
DESCRIPTOR.message_types_by_name['Stream'] = _STREAM

Hello = _reflection.GeneratedProtocolMessageType('Hello', (_message.Message,), dict(
  DESCRIPTOR = _HELLO,
  __module__ = 'sync_pb2'
  # @@protoc_insertion_point(class_scope:distdb.sync.Hello)
  ))
_sym_db.RegisterMessage(Hello)

Setup = _reflection.GeneratedProtocolMessageType('Setup', (_message.Message,), dict(
  DESCRIPTOR = _SETUP,
  __module__ = 'sync_pb2'
  # @@protoc_insertion_point(class_scope:distdb.sync.Setup)
  ))
_sym_db.RegisterMessage(Setup)

Update = _reflection.GeneratedProtocolMessageType('Update', (_message.Message,), dict(
  DESCRIPTOR = _UPDATE,
  __module__ = 'sync_pb2'
  # @@protoc_insertion_point(class_scope:distdb.sync.Update)
  ))
_sym_db.RegisterMessage(Update)

Checkpoint = _reflection.GeneratedProtocolMessageType('Checkpoint', (_message.Message,), dict(
  DESCRIPTOR = _CHECKPOINT,
  __module__ = 'sync_pb2'
  # @@protoc_insertion_point(class_scope:distdb.sync.Checkpoint)
  ))
_sym_db.RegisterMessage(Checkpoint)

Stream = _reflection.GeneratedProtocolMessageType('Stream', (_message.Message,), dict(
  DESCRIPTOR = _STREAM,
  __module__ = 'sync_pb2'
  # @@protoc_insertion_point(class_scope:distdb.sync.Stream)
  ))
_sym_db.RegisterMessage(Stream)


# @@protoc_insertion_point(module_scope)
