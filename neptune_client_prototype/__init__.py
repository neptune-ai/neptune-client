#####################
### Neptune types
#####################

class Typ:
  pass

class BuiltInType(Typ):

  def __init__(self, value):
    super().__init__()
    self.value = value

  def __eq__(self, other):
    return self.value == other.value

class Integer(BuiltInType):
  pass

class Float(BuiltInType):
  pass

class String(BuiltInType):
  pass

class Boolean(BuiltInType):
  pass

# TODO add custom types

###################################
### Mock for testing
###################################

last_op = [None]

###################################
### Variables and structures
###################################

# TODO In the specification, a Variable does not hold data directly, 
# but instead hold a Structure object. The current implementation requires
# fewer delegated method calls, but perhaps there are other requirements
# which justify the existence of a standalone Structure type.
#
# TODO should a Namespace know the path to self?
class Variable:
  
  def __init__(self):
    super().__init__()
    # TODO confirming: parameter type stays fixed once the structure is created?
    # TODO need to determine the type while creating the Variable?
    self.typ = None

class Atom(Variable):
  
  def __init__(self):
    super().__init__()

  def assign(self, value):
    last_op[0] = ('atom.assign', convert_type(self.__class__, value))

class Series(Variable):
  
  def __init__(self):
    # TODO check that the type is supported by the Series structure 
    super().__init__() 

  def log(self, value):
    last_op[0] = ('series.log', convert_type(self.__class__, value))

class Set(Variable):
  
  def __init__(self):
    # TODO check that the type is supported by the Set structure 
    super().__init__() 

  def add(self, value):
    last_op[0] = ('set.add', convert_type(self.__class__, value))

###################################
### Implicit mappings
###################################

# FIXME We provide a global list of conversions in the prototype.
# Ideally, there should be a way to register custom conversions.
# Also, lookup should be made efficient.

# The format is (structure_type, external_type, neptune_conversion)
implicit_conversions = [
  (Variable, float, Float),
  (Variable, bool, Boolean),
  (Variable, str, String),
  (Atom, int, Integer),
  (Series, int, Float)
]

def convert_type(structure_type, value):
  for (st, et, conversion) in implicit_conversions:
    if issubclass(structure_type, st) and isinstance(value, et):
      return conversion(value)
  else:
    raise ValueError()

###################################
### Stub
###################################

class Stub:
  """A stub is used to enable behavior whereby calling
  
  >>> npt['foo/bar'].log(42)

  creates a Series named 'bar' even if it hasn't previously existed.
  """

  # TODO is holding a reference to the parent namespace and own key in that
  # namespace an acceptable hack?
  def __init__(self, parent, key):
    super().__init__()
    self.parent = parent
    self.key = key

  def log(self, *args, **kwargs):
    # TODO do we have to determine the type of the Series here?
    series = Series()
    self.parent[self.key] = series
    series.log(*args, **kwargs)

  def add(self, *args, **kwargs):
    set_ = Set()
    self.parent[self.key] = set_
    set_.add(*args, **kwargs)

  def assign(self, *args, **kwargs):
    atom = Atom()
    self.parent[self.key] = atom
    atom.assign(*args, **kwargs)

###################################
### Namespace
###################################

# TODO should a Namespace know the path to self?
class Namespace:

  def __init__(self, **kwargs):
    super().__init__()
    self._members = {}
    for k, v in kwargs.items():
      if isinstance(v, Namespace) or isinstance(v, Variable):
        self._members[k] = v
      elif isinstance(v, dict):
        self._members[k] = Namespace(**v)
      else:
        raise TypeError()

  def __contains__(self, key):
    # TODO support paths like foo/bar here or treat as implementation details?
    return key in self._members

  # TODO path validation, empty path
  def __getitem__(self, path):
    split_path = path.split('/', maxsplit=1)
    segment = split_path[0]

    if not segment: # empty string
      raise ValueError()

    if len(split_path) == 1:
      if segment in self._members:
        return self._members[segment]
      else:
        self._members[segment] = Stub(self._members, segment)
        return self._members[segment]
    else:
      rest = split_path[1]
      if segment in self._members:
        return self._members[segment][rest]
      else:
        self._members[segment] = Namespace()
        return self._members[segment][rest]

  def __setitem__(self, path, value):
    self[path].assign(value)

  # TODO: implement batch update methods: assign, log, add
  # after resolving https://docs.google.com/document/d/1aWcBYtaoXh9cuXBvVASISIvgSOW0zs8bygJG2ACcoOQ/edit?disco=AAAAGpSvrFM



# FIXME: Inconstent interface
# Given
# >>> npt = Namespace()
# We can do
# >>> npt['foo/bar'] = 42
# But we cannot do
# >>> v = npt['foo/bar']
# >>> v = 42
