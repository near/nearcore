const superagent = require('superagent');

class LocalNodeConnection {
    async request(methodName, params) {
        const response = await superagent
            .post(`http://localhost:3030/${methodName}`)
            .use(require('superagent-logger'))
            .send(params);
        return JSON.parse(response.text);
    };
}

module.exports = LocalNodeConnection;