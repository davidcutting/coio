# Google Benchmark: system install (nixpkgs gbenchmark) or a wrapdb subproject.
benchmark_dep = dependency('benchmark')
benchmark_main = cpp.find_library('benchmark_main', required : false)
bench_deps = [kioto_dep, benchmark_dep]
if benchmark_main.found()
  bench_deps += benchmark_main
endif

bench_names = [
  'bench_accept',
  'bench_io',
  'bench_metrics',
  'bench_probe_sweep',
  'bench_udp',
  'bench_udp_crosscore',
]

foreach name : bench_names
  executable(name, files(name + '.cpp'), dependencies : bench_deps)
endforeach
