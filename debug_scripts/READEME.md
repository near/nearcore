# Debug Scripts
## Content
* request_chain_info.py

  This script can be used to request blockchain info
* send_validator_logs.py


  This script can be used to send Validator logs to a Pagoda  S3 bucket when issues are encountered. The pagoda team can use the logs to help the validators troubleshoot issues.


## Instruction to RUN

  ```
  cd <path-to-nearcore>/nearcore/debug_scripts
  python3 -m pip install pipenv
  python3 -m pipenv shell
  python3 send_validator_logs.py --help
  OR
  python3 request_chain_info.py --help
  ```

## Instruction to run test
Add nearcore/debug_scripts to your PYTHONPATH
```
export PYTHONPATH="<absolute path>/nearcore/debug_scripts:$PYTHONPATH"
```

```
cd <absolute path>/nearcore/debug_scripts
python3 -m pipenv sync
python3 -m pipenv shell
python3 -m unittest tests.send_validator_logs_test 
```


