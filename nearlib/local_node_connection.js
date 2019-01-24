const sendJson = require('./internal/send-json');

class LocalNodeConnection {
    constructor (baseUrl) {
        this.baseUrl = baseUrl;
    }

    async request(methodName, params) {
        return await sendJson('POST', `${this.baseUrl}/${methodName}`, params);
    }
}

module.exports = LocalNodeConnection;
