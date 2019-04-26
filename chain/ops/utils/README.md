# Setup

## Install python requirements

```bash
pip install -r requirements.txt
```

## Install kubectl

**MacOS**
```bash
brew install kubectl
```

**Ubuntu**
```bash
sudo snap install kubectl --classic
```

## Setup AWS Credentials (optional)

If you do not have your key pair set in a
[credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html),
you will need to have `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set when running `build_and_run`.

## Install minikube (local only)
1) [Install virtualbox](https://www.virtualbox.org/wiki/Downloads)

2) [Install minikube](https://github.com/kubernetes/minikube/releases)

3) Configure minikube
```bash
minikube addons enable ingress
minikube addons enable registry
``` 
