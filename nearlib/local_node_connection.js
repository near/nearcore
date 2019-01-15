const superagent = require('superagent');

class LocalNodeConnection {
    constructor (baseUrl) {
        this.baseUrl = baseUrl;
    }

    async request(methodName, params) {
        const response = await superagent
            .post(`${this.baseUrl}/${methodName}`)
            .use(require('superagent-logger'))
            .send(params);
        return JSON.parse(response.text);
    };
}

module.exports = LocalNodeConnection;