project('stdexec', 'cpp', version : '0.1.0', license : 'Apache-2.0 WITH LLVM-exception')

# NVIDIA/stdexec is a header-only reference implementation of P2300. All that a
# consumer needs is its include/ dir and C++20 (enforced by the parent project).
stdexec_dep = declare_dependency(
  include_directories : include_directories('include'),
)
meson.override_dependency('stdexec', stdexec_dep)
