# MISC
To run tests locally on linux, run:
```sh
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/microsoft/servicefabric/bin/Fabric/Fabric.Code:"
```

# Packaging tests
```sh
python -m unittest discover -s scripts -p "test_*.py"
```