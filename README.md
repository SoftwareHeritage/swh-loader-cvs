swh-loader-cvs
==============

The Software Heritage CVS Loader is a tool and a library to walk a local CVS repository
and inject into the SWH dataset all contained files that weren't known before.

The main entry points are

- :class:`swh.loader.cvs.loader.CvsLoader` for the main cvs loader which ingests content out of
  a local cvs repository

# CLI run

With the configuration:

/tmp/loader_cvs.yml:
```
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```

Run:

```
swh loader --config-file /tmp/loader_cvs.yml \
    run cvs <cvs-repository-module-path>
```
