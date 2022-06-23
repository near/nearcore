import argparse
import shlex
import random
import sys
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import mocknet
from configured_logger import logger


def restart_restaked_script(restaked_url, delay_sec):
    s = '''
        killall restaked
        wget '{restaked_url}' -O /home/ubuntu/restaked
        nohup ./restaked --home /home/ubuntu/.near/shardnet/ --rpc-url 127.0.0.1:3030 --wait-period {delay_sec} --stake-amount {stake_amount} 1>>/home/ubuntu/restaked.out 2>>/home/ubuntu/restaked.err </dev/null &
    '''.format(restaked_url=shlex.quote(restaked_url),
               stake_amount=shlex.quote(str(random.randint(10**3, 10**5))),
               delay_sec=shlex.quote(str(delay_sec)))
    logger.info(f'Starting restaked: {s}')
    return s


def create_account(node, near_pk, near_sk):
    logger.info(f'Create account on {node.instance_name}')
    node.machine.upload('tests/shardnet/scripts/create_account.sh',
                        '/home/ubuntu',
                        switch_user='ubuntu')
    s = '''
        bash /home/ubuntu/create_account.sh {near_pk} {near_sk}
    '''.format(near_pk=shlex.quote(near_pk), near_sk=shlex.quote(near_sk))
    logger.info(f'Creating an account: {s}')
    node.machine.run('bash', input=s)


def restart_restaked(node, restaked_url, delay_sec, near_pk, near_sk,
                     create_accounts):
    if create_accounts and not node.instance_name.startswith('shardnet-boot'):
        create_account(node, near_pk, near_sk)
    logger.info(f'Restarting restaked on {node.instance_name}')
    node.machine.run('bash',
                     input=restart_restaked_script(restaked_url, delay_sec))


if __name__ == '__main__':
    logger.info('Starting restaker.')
    parser = argparse.ArgumentParser(description='Run restaker')
    parser.add_argument('--restaked-url', required=True)
    parser.add_argument('--delay-sec', type=int, required=True)
    parser.add_argument('--near-pk', required=True)
    parser.add_argument('--near-sk', required=True)
    parser.add_argument('--create-accounts', default=False, action='store_true')
    args = parser.parse_args()

    restaked_url = args.restaked_url
    delay_sec = args.delay_sec
    assert restaked_url
    assert delay_sec
    near_pk = args.near_pk
    near_sk = args.near_sk
    create_accounts = args.create_accounts

    all_machines = mocknet.get_nodes(pattern='shardnet-')
    random.shuffle(all_machines)

    pmap(
        lambda machine: restart_restaked(machine, restaked_url, delay_sec,
                                         near_pk, near_sk, create_accounts),
        all_machines)
