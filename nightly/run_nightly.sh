while true
do
        git pull --ff-only origin staging
        cd .. && cargo build --all --tests && cd nightly
        timestamp=$(date +%y%m%d_%H%M%S)
        output_path=~/testruns/$(git rev-parse HEAD)_${timestamp}
        python nightly.py run nightly.txt ${output_path}
        gsutil cp -h -r ${output_path} gs://log.nightly.neartest.com
        find output_path -type f ! -name log -exec rm {} \;
done
