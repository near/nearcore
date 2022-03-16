window.onload = () => {
    const estimationSelect = document.getElementById('estimations');
    const protocolSelect = document.getElementById('protocol-versions');
    estimationSelect.addEventListener('change', onSelectEstimation);
    protocolSelect.addEventListener('change', onSelectProtocol);
    // Populate select forms with all estimations available 
    fetchDirectory().then(
        directory => {
            directory.estimations.forEach(name => estimationSelect.add(new Option(name)));
            directory.protocols.forEach(protocolVersion => protocolSelect.add(new Option(protocolVersion)));
            const uiState = window.history.state;
            protocolSelect.value = uiState.selectedProtocol;
        }
    );

    // Read URL to display preselected estimations
    const urlParams = new URLSearchParams(window.location.search);
    const estimations = urlParams.get('estimations');
    const protocol = urlParams.get('protocol');

    let uiState = emptyUiState();
    if (estimations) {
        uiState.selectedEstimations = new Set(estimations.split(','));
    }
    if (protocol) {
        uiState.selectedProtocol = protocol;
    }
    updateUi(uiState);
}

function emptyUiState() {
    return {
        selectedEstimations: new Set(),
        selectedProtocol: '-',
    }
}

// Update the ui state stored in `History.state` and the URL encoding of it.
// Always use this function to update the UI, it will make sure the browser URL
// is shareable.
function updateUi(uiState) {
    const urlComponents = {};
    if (uiState.selectedEstimations.size > 0) {
        urlComponents.estimations = Array.from(uiState.selectedEstimations);
    }
    if (uiState.selectedProtocol !== '-') {
        urlComponents.protocol = uiState.selectedProtocol;
    }
    let url = '?';
    if (Object.keys(urlComponents).length !== 0) {
        for (key in urlComponents) {
            url += key + '=' + urlComponents[key] + '&';
        }
        url = url.slice(0, -1);
    }
    window.history.replaceState(uiState, '', url);
    renderUi(uiState);
}

// Caches data already fetched in a ad-hoc cache. Once prototyping phase is
// done, this could be replaced with session cache or even local storage.
const IN_MEMORY_CACHE = {
    chart: undefined,
    directory: undefined,
    parameters: {},
    estimations: {},
};

function chart() {
    if (IN_MEMORY_CACHE.chart === undefined) {
        const chartDom = document.getElementById('chart');
        IN_MEMORY_CACHE.chart = echarts.init(chartDom);
    }
    return IN_MEMORY_CACHE.chart;
}

function fetchDirectory() {
    return new Promise((resolve, reject) => {
        if (IN_MEMORY_CACHE.directory !== undefined) {
            resolve(IN_MEMORY_CACHE.directory);
        } else {
            fetch('_directory.json')
                .then(response => response.json())
                .then(parsedData => {
                    IN_MEMORY_CACHE.directory = parsedData;
                    resolve(IN_MEMORY_CACHE.directory);
                })
                .catch((err) => reject(err));
        }
    });
}

function fetchParameters(protocol) {
    if (protocol === '-') {
        return Promise.resolve(undefined);
    }
    return new Promise((resolve, reject) => {
        if (IN_MEMORY_CACHE.parameters[protocol]) {
            resolve(IN_MEMORY_CACHE.parameters[protocol]);
        } else {
            fetch(`_protocol_v${protocol}.json`)
                .then(response => response.json())
                .then(parsedData => {
                    IN_MEMORY_CACHE.parameters[protocol] = parsedData;
                    resolve(IN_MEMORY_CACHE.parameters[protocol]);
                })
                .catch((err) => reject(err));
        }
    });
}

function fetchEstimatedData(estimation) {
    return new Promise((resolve, reject) => {
        if (IN_MEMORY_CACHE.estimations[estimation]) {
            resolve(IN_MEMORY_CACHE.estimations[estimation]);
        } else {
            fetch(`${estimation}.json`)
                .then(response => response.json())
                .then(parsedData => {
                    IN_MEMORY_CACHE.estimations[estimation] = parsedData;
                    resolve(IN_MEMORY_CACHE.estimations[estimation]);
                })
                .catch((err) => reject(err));
        }
    });
}

function renderUi(uiState) {
    const selectedEstimations = Array.from(uiState.selectedEstimations);

    if (selectedEstimations.length === 0) {
        return;
    }

    const protocolDataPromise = fetchParameters(uiState.selectedProtocol);
    const estimationPromises = selectedEstimations.map(
        name => fetchEstimatedData(name).then(data => { return { name, data } })
    );

    Promise.all([protocolDataPromise, ...estimationPromises]
    ).then(data => {
        const protocolData = data[0];
        const estimationsDataArray = data.slice(1);
        plot(estimationsDataArray, protocolData);
    });
}

function plot(estimationsDataArray, protocolData, reset = true) {
    if (!(estimationsDataArray instanceof Array) || estimationsDataArray.length == 0) {
        console.error('No Data provided');
        return;
    }
    const xData = Object.keys(estimationsDataArray[0].data);
    const xLabels = xData.map((x) => x.substring(0, 7));
    const yEstimationsRows = estimationsDataArray.map((row) => estimationSeries(row.name, xData, row.data));
    let yParameterRows = [];
    if (protocolData) {
        yParameterRows = estimationsDataArray.map((row) => parameterSeries(row.name, xData, protocolData[row.name]));
    }

    const option = {
        xAxis: {
            type: 'category',
            data: xLabels,
            name: 'Commit'
        },
        yAxis: {
            type: 'value',
            name: 'Mgas ~ ns'
        },
        legend: {
            orient: 'vertical',
            right: 0,
            top: 'center'
        },
        series: [...yEstimationsRows, ...yParameterRows],
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
    const uiState = window.history.state;
    if (singular) {
        uiState.selectedEstimations.clear();
    }
    uiState.selectedEstimations.add(event.target.value);
    updateUi(uiState);
}

// Event listener for select form that chooses protocol version to be displayed
function onSelectProtocol(event) {
    const uiState = window.history.state;
    uiState.selectedProtocol = event.target.value;
    updateUi(uiState);
}

// Event listener for checkbox that changes between singular / multiple estimations in chart
function onSingularChange() {
    const uiState = window.history.state;
    if (document.getElementById('singular').checked) {
        uiState.selectedEstimations = new Set();
        const selectedNow = document.getElementById('estimations').value;
        if (selectedNow !== '-') {
            uiState.selectedEstimations.add(selectedNow);
        }
        updateUi(uiState);
    }
}

// Event listener for form reset button
function onReset() {
    updateUi(emptyUiState());
    chart().clear();
}

// Creates an echarts line series object from an estimation data object
function estimationSeries(name, keys, data) {
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

// Creates an echarts line series for a parameter value
function parameterSeries(name, keys, gasValue) {
    let repeatedValue = keys.map(() => gasValue / 1000000 );
    return {
        name: 'Parameter(' + name + ')',
        data: repeatedValue,
        type: 'line',
        connectNulls: true,
        lineStyle: {
            type: 'dashed',
        }
    };
}