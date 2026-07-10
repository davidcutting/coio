project('kioto', 'cpp',
  version : '0.11.0',
  license : 'MIT',
  meson_version : '>= 1.1.0',
  default_options : [
    'cpp_std=c++20',        # NVIDIA backend needs C++20; BEMAN/CXX26 need -Dcpp_std=c++23 / c++26
    'default_library=static',
    'warning_level=3',
  ],
)

cpp = meson.get_compiler('cpp')
system = host_machine.system()
backend = get_option('senders_backend')

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
# public_args propagate to consumers of kioto_dep; private_args are kioto-only.
public_args = ['-DKIOTO_EXECUTION_USE_' + backend]
private_args = []
link_args = []

deps = [dependency('threads')]

# std::execution backend. NVIDIA/stdexec is header-only and vendored via
# subprojects/stdexec.wrap when not already installed (see [provide] in the wrap).
if backend == 'NVIDIA'
  deps += dependency('stdexec')
elif backend == 'BEMAN'
  deps += dependency('beman-execution')  # supply a system install or a subprojects/beman-execution.wrap
endif
# CXX26: no external dependency; uses the standard library <execution>.

# io_uring (Linux). Optional: when absent, the io_uring driver is compiled out
# at the header level (KIOTO_HAS_IO_URING), matching the CMake behaviour.
if system == 'linux'
  liburing_dep = dependency('liburing', required : false)
  if liburing_dep.found()
    deps += liburing_dep
  else
    message('liburing not found: the io_uring driver will be unavailable')
  endif
elif system == 'windows'
  deps += cpp.find_library('ws2_32')
  deps += cpp.find_library('mswsock')
  private_args += ['-DWIN32_LEAN_AND_MEAN', '-DNOMINMAX']
endif

if cpp.get_id() == 'msvc'
  public_args += ['/utf-8', '/permissive-', '/Zc:preprocessor']
endif

# mimalloc as the default allocator (measured 1.5-3.3x on spawn-heavy throughput;
# neutral on the allocation-free datagram path). kioto references no mimalloc
# symbol directly (it calls malloc, resolved at runtime), so --no-as-needed is
# required or the linker drops the DT_NEEDED. Skipped under sanitizers, which
# install their own malloc interceptors.
mimalloc_opt = get_option('mimalloc')
if not mimalloc_opt.disabled() and get_option('b_sanitize') == 'none'
  mimalloc_dep = dependency('mimalloc', required : mimalloc_opt.enabled())
  if mimalloc_dep.found()
    mi_link = ['-Wl,--no-as-needed', '-lmimalloc', '-Wl,--as-needed']
    libdir = mimalloc_dep.get_variable(pkgconfig : 'libdir', default_value : '')
    if libdir != ''
      mi_link += '-L' + libdir
    endif
    link_args += mi_link
  endif
endif

# ---------------------------------------------------------------------------
# Sources (module-first; platform impls selected by filename suffix)
# ---------------------------------------------------------------------------
sources = files('src/kioto/base/error.cpp')  # portable

if system == 'linux'
  sources += files(
    'src/kioto/base/basic.linux.cpp',
    'src/kioto/base/signal_wait.linux.cpp',
    'src/kioto/io/driver/epoll_context.linux.cpp',
    'src/kioto/io/driver/uring_context.linux.cpp',
    'src/kioto/io/file.linux.cpp',
    'src/kioto/io/pipe.linux.cpp',
    'src/kioto/net/resolver.linux.cpp',
    'src/kioto/net/socket.linux.cpp',
    'src/kioto/net/tcp.linux.cpp',
    'src/kioto/net/udp.linux.cpp',
  )
elif system == 'windows'
  sources += files(
    'src/kioto/base/basic.win.cpp',
    'src/kioto/base/signal_wait.win.cpp',
    'src/kioto/io/driver/iocp_context.win.cpp',
    'src/kioto/io/file.win.cpp',
    'src/kioto/io/pipe.win.cpp',
    'src/kioto/net/resolver.win.cpp',
    'src/kioto/net/socket.win.cpp',
    'src/kioto/net/tcp.win.cpp',
    'src/kioto/net/udp.win.cpp',
  )
endif

public_inc = include_directories('include')
private_inc = include_directories('src')  # internal impl headers: kioto/common.<plat>.h

# ---------------------------------------------------------------------------
# Library + exported dependency
# ---------------------------------------------------------------------------
kioto_lib = library('kioto',
  sources,
  include_directories : [public_inc, private_inc],
  dependencies : deps,
  cpp_args : public_args + private_args,
  link_args : link_args,
  install : true,
)

install_subdir('include/kioto', install_dir : get_option('includedir'))

# The dependency a parent project consumes:
#   kioto_dep = subproject('kioto').get_variable('kioto_dep')
# or, with a wrap providing 'kioto':  dependency('kioto')
kioto_dep = declare_dependency(
  link_with : kioto_lib,
  include_directories : public_inc,
  dependencies : deps,
  compile_args : public_args,
  link_args : link_args,
)
meson.override_dependency('kioto', kioto_dep)

import('pkgconfig').generate(kioto_lib,
  name : 'kioto',
  description : 'C++20 async I/O (io_uring/epoll/iocp) sender/receiver runtime',
  extra_cflags : public_args,
)

if get_option('tests')
  subdir('tests')
endif
if get_option('benchmarks')
  subdir('bench')
endif
if get_option('examples')
  subdir('examples')
endif
