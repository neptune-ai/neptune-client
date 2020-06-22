from . import Namespace, last_op, Integer, Float, String, Boolean

def test():
  npt = Namespace()
  npt['path/to/variable1'] = 1
  assert last_op[0] == ('atom.assign', Integer(1))
  npt['path/to/variable2'].assign(1.23)
  assert last_op[0] == ('atom.assign', Float(1.23))
  npt['path/to/variable3'].log(1)
  assert last_op[0] == ('series.log', Float(1))
  npt['path/ns/variable'].add('tag')
  assert last_op[0] == ('set.add', String('tag'))