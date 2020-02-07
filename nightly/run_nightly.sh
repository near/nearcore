while true
do
        git pull --ff-only origin staging
        cd .. && cargo build --all --tests && cd nightly
        timestamp=$(date +%y%m%d_%H%M%S)
        output_path=~/testruns/$(git rev-parse HEAD)_${timestamp}
        python nightly.py run nightly.txt ${output_path}

        # Upload file bigger than 1M to gcloud storage
        for f in `find ${output_path} -type f -size +1M`
        do
                path=$(echo ${f} | sed "s|.*testruns/||");
                cd ~/testruns
                gsutil cp -r ${path} gs://log.nightly.neartest.com/${path}
                # Original log save the log url
                echo "https://storage.cloud.google.com/log.nightly.neartest.com/${path}" > ${f}
                cd -
        done
done
