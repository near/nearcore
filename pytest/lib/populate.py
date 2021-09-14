import subprocess
from os.path import join
from shutil import copy2, rmtree


def genesis_populate(near_root, additional_accounts, node_dir):
    subprocess.check_call(
        (join(near_root, 'genesis-populate'), '--additional-accounts-num',
         str(additional_accounts), '--home', node_dir))
    rmtree(join(node_dir, 'data'), ignore_errors=True)


def copy_genesis(node_dir_source, node_dir_target):
    for file_name in ['genesis.json', 'genesis_roots', 'state_dump']:
        source_file = join(node_dir_source, file_name)
        target_file = join(node_dir_target, file_name)
        copy2(source_file, target_file)


def genesis_populate_all(near_root, additional_accounts, node_dirs):
    genesis_populate(near_root, additional_accounts, node_dirs[0])
    for node_dir in node_dirs[1:]:
        copy_genesis(node_dirs[0], node_dir)
        rmtree(join(node_dir, 'data'), ignore_errors=True)
