# pytest

## Run all tests on a single remote node
0. Locally in virtualenv install requirements.txt, have `python >= 3.6`
1. `./create_single_machine.py --cpu=32` # or other desired #CPUs
2. `gcloud compute ssh pytest-node`
3. 
```
cd nearcore/pytest
. venv/bin/activate
./run_all_tests.py
```
4. If anything test fails, logs will be in `pytest/failed_tests_output/`