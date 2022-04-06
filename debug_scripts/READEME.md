# Debug Scripts
## Content
* reqeust_chain_info.py

  This script can be used to request blockchain info
* send_validator_logs.py


  This script can be used to send Validator logs to a Pagoda  S3 bucket when issues are encountered. The pagoda team can use the logs to help the validators troubleshoot issues.


## Instruction to RUN

  Go to debug_scripts directory
  ```
  cd <path-to-nearcore>/nearcore/debug_scripts
  ```

  Install pipenv

  ```
  python3 -m pip install pipenv
  ```

  ```
  python3 -m pipenv shell
  ```

  ```
  python send_validator_logs.py --help
  ```
  OR

  ```
  python request_chain_info.py --help
  ```

