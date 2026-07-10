executable('example-http_server',
  files('connection.cpp', 'response.cpp', 'router.cpp', 'main.cpp'),
  dependencies : kioto_dep,
  cpp_args : ['-DHTTP_SERVER_STATIC_DIR="' + meson.current_source_dir() + '/static"'],
)
