while true
do
        git pull --ff-only origin staging
        cd .. && cargo build --all --tests --features adversarial && cd nightly
        python nightly.py run nightly.txt ~/testruns/$(git rev-parse HEAD)_$(date +%y%m%d_%H%M%S)
done
