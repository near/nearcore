let fetch = (typeof window === 'undefined' || window.name == 'nodejs') ? require('node-fetch') : window.fetch;
module.exports = async function sendJson(method, url, json) {
    const response = await fetch(url, {
        method: method,
        body: method != "GET" ? JSON.stringify(json) : undefined,
        headers: { 'Content-type': 'application/json; charset=utf-8' }
    });
    if (!response.ok) {
        throw new Error(await response.text());
    }
    if (response.status === 204) {
        // No Content
        return null;
    }
    return await response.json();
}