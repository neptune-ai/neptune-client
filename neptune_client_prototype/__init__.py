

###################################
### Implicit mappings
###################################

# FIXME We provide a global list of conversions in the prototype.
# Ideally, there should be a way to register custom conversions.
# Also, lookup should be made efficient.

# # The format is (structure_type, external_type, neptune_conversion)
# implicit_conversions = [
#     (Atom, int, int),
#     (Series, int, float)
# ]

# def convert_type(structure_type, value):
#     for (st, et, conversion) in implicit_conversions:
#         if issubclass(structure_type, st) and isinstance(value, et):
#             return conversion(value)
#     raise ValueError()

###################################
### Namespace
###################################
