import os
import logger
import subprocess

##########################################################################################

def main() -> None:
    logger.info(f"Current environment: \n{os.environ}")
    # proc = subprocess.Popen(
    #         [
    #             'cargo',
    #             'fuzz',
    #             'build',
    #             self.target['runner'],
    #         ],
    #         cwd=repo_dir / target['crate'],
    #         start_new_session=True,
    #         stdout=self.log_file,
    #         stderr=subprocess.STDOUT,
    #     )

if __name__ == '__main__':
    # os.environ['RUSTC_BOOTSTRAP'] = '1'  # Nightly is needed by cargo-fuzz
    main()
