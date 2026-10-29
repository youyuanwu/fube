"""Run the complete client-runtime partition from tests/suites.toml."""

from tests import run


def main(argv=None):
    return run.main(argv, partition="client-runtime")


if __name__ == "__main__":
    raise SystemExit(main())
