window.onload = () => {
    // Populate select form with all estimations available 
    fetch_directory().then(
        directory => {
            directory.names.forEach(name => select.add(new Option(name)));
        }
    );

    // Add event listener to display selected estimation
    const select = document.getElementById('estimations');
    select.addEventListener('change', onSelectEstimation);

    // Read URL to display preselected estimations
    const urlParams = new URLSearchParams(window.location.search);
    const estimations = urlParams.get('estimations');

    let state;
    if (estimations) {
        state = new Set(estimations.split(','));
    } else {
        state = new Set();
    }
    window.history.replaceState(state, '');
    plot_from_state();
}

const IN_MEMORY_CACHE = {
    chart: undefined,
    directory: undefined,
    estimations: {}
};

function chart() {
    if (IN_MEMORY_CACHE.chart === undefined) {
        const chartDom = document.getElementById('chart');
        IN_MEMORY_CACHE.chart = echarts.init(chartDom);
    }
    return IN_MEMORY_CACHE.chart;
}

function fetch_directory() {
    return new Promise((resolve, reject) => {
        if (IN_MEMORY_CACHE.directory !== undefined) {
            resolve(IN_MEMORY_CACHE.directory);
        } else {
            fetch('_directory.json')
                .then(response => response.json())
                .then(parsed_data => {
                    IN_MEMORY_CACHE.directory = parsed_data;
                    resolve(IN_MEMORY_CACHE.directory);
                })
                .catch((err) => reject(err));
        }
    });
}

function fetch_estimated_data(estimation) {
    return new Promise((resolve, reject) => {
        if (IN_MEMORY_CACHE.estimations[estimation]) {
            resolve(IN_MEMORY_CACHE.estimations[estimation]);
        } else {
            fetch(`${estimation}.json`)
                .then(response => response.json())
                .then(parsed_data => {
                    IN_MEMORY_CACHE.estimations[estimation] = parsed_data;
                    resolve(IN_MEMORY_CACHE.estimations[estimation]);
                })
                .catch((err) => reject(err));
        }
    });
}

// Read the `Set` of estimations to be displayed from `History.state` and plot them
function plot_from_state() {
    const selectedEstimations = Array.from(window.history.state);

    if (selectedEstimations.length === 0) {
        return;
    }

    Promise.all(
        selectedEstimations.map(
            name => fetch_estimated_data(name).then(data => { return { name, data } })
        )
    ).then(dataArray => plot(dataArray));
}

function plot(dataArray, reset = true) {
    if (!(dataArray instanceof Array) || dataArray.length == 0) {
        console.error('No Data provided');
        return;
    }
    const xData = Object.keys(dataArray[0].data);
    const yData = dataArray.map((row) => toSeries(row.name, xData, row.data))
    const labels = xData.map((x) => x.substring(0, 7));

    const option = {
        xAxis: {
            type: 'category',
            data: labels
        },
        yAxis: {
            type: 'value'
        },
        legend: {
            orient: 'vertical',
            right: 0,
            top: 'center'
        },
        series: yData,
        tooltip: {
            position: 'inside',
            trigger: 'item',
        }
    };

    chart().setOption(option, reset);
}

// Event listener for select form that chooses estimations to be displayed
function onSelectEstimation(event) {
    if (event.target.value === '-') {
        return;
    }
    const singular = document.getElementById('singular').checked;
    const selectedEstimations = window.history.state;
    if (singular) {
        selectedEstimations.clear();
    }
    selectedEstimations.add(event.target.value);
    const url = '?estimations=' + encodeURIComponent(Array.from(selectedEstimations));
    window.history.replaceState(selectedEstimations, '', url);
    plot_from_state();
}

// Event listener for checkbox that changes between singular / multiple estimations in chart
function onSingularChange() {
    if (document.getElementById('singular').checked) {
        const selectedEstimations = new Set();
        const selectedNow = document.getElementById('estimations').value;
        if (selectedNow !== '-') {
            selectedEstimations.add(selectedNow);
            const url = '?estimations=' + encodeURIComponent(Array.from(selectedEstimations));
            window.history.replaceState(selectedEstimations, '', url);
        } else {
            window.history.replaceState(selectedEstimations, '', '?');
        }
        plot_from_state();
    }
}

// Event listener for form reset button
function onReset() {
    window.history.replaceState(new Set(), '', '?');
    chart().clear();
}

// Creates an echarts line series object
function toSeries(name, keys, data) {
    let estimations = keys.map(
        (x) => x in data ? data[x].gas / 1000000 : '-'
    );
    return {
        name: name,
        data: estimations,
        type: 'line',
        step: true,
    };
}