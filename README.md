# antares-client

A light-weight client for receiving alerts from
[ANTARES](http://antares.noao.edu).

ANTARES is an Alert Broker developed by the [NOAO](http://noao.edu) for ZTF and
LSST.

The client is available for use as a Python library and as a command line tool.
Directions for both use cases follow in the [Usage](#usage) section.

Table of Contents:
* [Installation](#installation)
* [Documentation](#documentation)
* [Upgrading](#upgrading)
* [Troubleshooting](#troubleshooting)

## Installation

The ANTARES client supports Python versions 3.6 and up.

To install:

```bash
$ pip install antares-client
```

Verify the client installed correctly:

```bash
$ antares --version
antares, version v1.1.0
```

If installing with Python 3.10, `librdkafka` is required:

Mac: `brew install librdkafka`

Ubuntu: `apt-get install librdkafka-dev`

## Documentation

Visit the [full documentation](https://noao.gitlab.io/antares/client) for usage
guides, API reference docs, and more.

## Troubleshooting

Issues? See the
[documentation](https://noao.gitlab.io/antares/client/troubleshooting) for
common gotchas and, if you can't find a solution there, please open an issue.

