# PinchMe
A PASTA data package and resource integrity checker.
```shell
Usage: pinchme [OPTIONS]

  PinchMe

  A PASTA data package and resource integrity checker. Running "pinchme" without any options will
  validate all new and existing data packages and resources, and then exit.

Options:
  -a, --algorithm TEXT   Package pool selection algorithm: either
                         'create_ascending', 'create_descending',
                         'id_ascending',  'id_descending', or 'random'
                         (default).
  -b, --bootstrap        Create a new resource database and run validation
                         against all packages.
  -d, --delay INTEGER    Delay (seconds) between package integrity checks.
  -e, --email            Email on integrity error.
  -f, --failed           Rerun integrity checks against all failed resources,
                         then exit.
  -i, --identifier TEXT  Add specified package to validation pool, then exit.
  -l, --limit INTEGER    Limit the number of new packages added to the
                         validation pool.
  -p, --pool             Add new packages to validation pool, then exit.
  -r, --reset            Reset validated flag on all packages/resources, then
                         exit.
  -s, --show             Show all failed resources. Error codes: 0b0001=MD5 ||
                         0b0010=SHA1 || 0b0100=SIZE || 0b1000=NOTFOUND
  -v, --validate TEXT    Validate specified package(s), then exit.
  -V, --verbose          Print activity to stdout.
  -h, --help             Show this message and exit.
```