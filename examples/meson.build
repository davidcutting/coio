# Single-file examples: one executable each, linking kioto.
example_names = [
  'async_latch',
  'cat',
  'cp',
  'fifo',
  'generator',
  'hello',
  'multi-threading',
  'pipe',
  'schedule',
  'scope',
  'task',
  'tcp_echo_client',
  'tcp_echo_server-context_pool',
  'tcp_echo_server-single_thread',
  'tcp_echo_server-thread_pool',
  'tcp_echo_server-with_timeout',
  'udp_echo_client',
  'udp_echo_server',
  'when_any',
]

foreach name : example_names
  executable('example-' + name, files(name + '.cpp'), dependencies : kioto_dep)
endforeach

subdir('http_server')
subdir('json_rpc')
