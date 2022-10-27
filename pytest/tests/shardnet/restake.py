import argparse
import shlex
import random
import sys
from retrying import retry, RetryError
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import mocknet
from configured_logger import logger


@retry(retry_on_result=bool, wait_fixed=2000, stop_max_attempt_number=3)
def create_account(node, near_pk, near_sk):
    node.machine.upload('tests/shardnet/scripts/create_account.sh',
                        '/home/ubuntu',
                        switch_user='ubuntu')
    s = '''
        bash /home/ubuntu/create_account.sh {near_pk} {near_sk} 1>/home/ubuntu/create_account.out 2>/home/ubuntu/create_account.err
    '''.format(near_pk=shlex.quote(near_pk), near_sk=shlex.quote(near_sk))
    logger.info(f'Creating an account on {node.instance_name}: {s}')
    result = node.machine.run('bash', input=s)
    if result.returncode != 0:
        logger.error(f'error running create_account.sh on {node.instance_name}')
    return result.returncode


def restart_restaked(node, delay_sec, near_pk, near_sk, need_create_accounts):
    if need_create_accounts and not node.instance_name.startswith(
            'shardnet-boot'):
        try:
            create_account(node, near_pk, near_sk)
        except RetryError:
            logger.error(
                f'Skipping stake step after errors running create_account.sh on {node.instance_name}'
            )
            return

    node.machine.upload('tests/shardnet/scripts/restaked.sh',
                        '/home/ubuntu',
                        switch_user='ubuntu')
    s = '''
        nohup bash ./restaked.sh {delay_sec} {stake_amount} 1>>/home/ubuntu/restaked.out 2>>/home/ubuntu/restaked.err </dev/null &
    '''.format(stake_amount=shlex.quote(str(random.randint(10**3, 10**5))),
               delay_sec=shlex.quote(str(delay_sec)))
    logger.info(f'Starting restaked on {node.instance_name}: {s}')
    node.machine.run('bash', input=s)


if __name__ == '__main__':
    logger.info('Starting restaker.')
    parser = argparse.ArgumentParser(description='Run restaker')
    parser.add_argument('--delay-sec', type=int, required=True)
    parser.add_argument('--near-pk', required=True)
    parser.add_argument('--near-sk', required=True)
    parser.add_argument('--create-accounts', default=False, action='store_true')
    args = parser.parse_args()

    delay_sec = args.delay_sec
    assert delay_sec
    near_pk = args.near_pk
    near_sk = args.near_sk
    need_create_accounts = args.create_accounts

    all_machines = mocknet.get_nodes(pattern='shardnet-')
    random.shuffle(all_machines)

    pmap(
        lambda machine: restart_restaked(machine, delay_sec, near_pk, near_sk,
                                         need_create_accounts), all_machines)
