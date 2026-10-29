"""Run the complete native-data CI partition, without the legacy runtime suite."""

from tests import run


def main(argv=None):
    return run.main(argv, partition="native-data")


if __name__ == "__main__":
    raise SystemExit(main())
