project('doctest', 'cpp', version : '2.5.2', license : 'MIT')

# doctest is a single header included as <doctest/doctest.h>; the repo root holds
# the doctest/ dir, so the root is the include path.
doctest_dep = declare_dependency(
  include_directories : include_directories('.'),
)
meson.override_dependency('doctest', doctest_dep)
