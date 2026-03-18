# common

## Config Forward Headers

`common/config.h` remains the single source of truth for BE config declarations:

- config names
- config types
- default values
- comments that describe the config

The generated `config_<module>_fwd.h` headers exist only to expose selected `CONF_*` declarations to consumers
without forcing them to include all of `common/config.h`.

### Why it works this way

We do this for two reasons:

1. `common/config.h` is large and widely included. Any direct include expands rebuild fanout when the file changes.
2. We do not want multiple hand-maintained config declaration files. That would split the source of truth and make
   defaults, types, and comments drift.

The current design keeps `config.h` authoritative and derives forward headers from it automatically:

- `common/config.h`: authoritative declarations, each `CONF_*` macro carries a **module annotation**
- `common/config.cpp`: definitions
- `build-support/gen_config_fwd_headers.py`: reads module annotations from `config.h` and generates `config_<module>_fwd.h`

The generator is invoked automatically by CMake whenever `config.h` or the script changes. Generated headers are
placed in `${GENSRC_DIR}/common/` (i.e. `be/src/gen_cpp/build/common/`) and are **not committed** to the source tree.
Consumer code includes them as `"common/config_<module>_fwd.h"` because `${GENSRC_DIR}` is on the include path.

### Module annotations

Each `CONF_*` macro call in `config.h` takes a module as its first argument:

```cpp
// Single module
CONF_mBool(cow, enable_cow_optimization, "true");

// Multiple modules — use | separator (valid C bitwise-OR; compiler ignores it)
CONF_Int32(network|staros_worker, be_port, "9060");
```

The module name becomes the suffix of the generated header: module `cow` → `config_cow_fwd.h`.
A config with `network|staros_worker` appears in **both** `config_network_fwd.h` and `config_staros_worker_fwd.h`.

`CONF_Alias` does not take a module argument; it inherits its target's module automatically.

### Rules

- Do not hand-edit `config_<module>_fwd.h` (they live in gensrc and are not committed).
- Add or modify config declarations only in `common/config.h` or a config fragment it includes.
- Always specify a module annotation when adding a new `CONF_*` entry.
- Prefer including `common/config_<module>_fwd.h` instead of `common/config.h` in headers and reusable `.cpp` files.

### How to add a new config

1. Add the new `CONF_*` entry in `be/src/common/config.h`, specifying the appropriate module:

```cpp
// single module
CONF_mInt32(storage, my_new_config, "42");

// belongs to two modules
CONF_mInt32(storage|lake, my_new_config, "42");
```

2. If the config belongs to a module that does not yet exist, just use a new module name — the build will
   automatically create `config_<newmodule>_fwd.h` on the next build.

3. Include the new forward header in consumers:

```cpp
#include "common/config_storage_fwd.h"
```

4. The headers are regenerated automatically during the next CMake build. To regenerate manually:

```bash
python3 build-support/gen_config_fwd_headers.py \
    --output-dir be/src/gen_cpp/build/common
```

### When a direct `common/config.h` include is acceptable

The preferred answer is "almost never" for new code.

Current legacy direct includes are tracked in the allowlist enforced by
`build-support/check_common_config_header_includes.sh`, but new direct includes should be treated as exceptions that
need strong justification. If you think a new direct include is necessary, first check whether:

- an existing `config_<module>_fwd.h` can be reused
- a new module annotation on the config would be cleaner

The migration path is to keep shrinking the allowlist over time, not to add new long-term direct includes.
