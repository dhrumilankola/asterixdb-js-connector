// File: /src/core/Connector.js

const axios = require('axios');

/**
 * Connector class to handle HTTP communication with AsterixDB.
 *
 * According to the AsterixDB HTTP API documentation:
 * 1. All queries (both read-only and data modification) are sent to the `/query/service` endpoint.
 *    - SELECT queries in synchronous mode are sent using a GET request.
 *    - DML queries (INSERT/UPDATE/DELETE) in synchronous mode are sent using a POST request.
 * 2. Asynchronous queries use additional endpoints:
 *    - `/query/service/status` to check the status of a submitted query.
 *    - `/query/service/result` to fetch the final results.
 */
class Connector {
  /**
   * Initializes the Connector with a hardcoded base URL for AsterixDB.
   */
  constructor() {
    this.baseURL = 'http://localhost:19002';
    this.httpClient = axios.create({
      baseURL: this.baseURL,
      timeout: 5000,
      headers: {
        'Content-Type': 'application/json',
      },
    });
  }

  /**
   * Executes a SQL++ query in synchronous mode.
   *
   * For SELECT queries, a GET request is sent to `/query/service`.
   * For DML queries (INSERT/UPDATE/DELETE), a POST request is sent to `/query/service`.
   *
   * @param {string} query - The SQL++ query string.
   * @returns {Promise<any>} The JSON response data from AsterixDB.
   * @throws {Error} If the HTTP request fails.
   */
  async executeQuery(query) {
    // Determine if the query is a DML query.
    const trimmedQuery = query.trim();
    let isDML = false;

    // Check for a USE statement and then for DML keywords.
    if (trimmedQuery.toUpperCase().startsWith("USE")) {
      const parts = trimmedQuery.split(";");
      isDML = parts.slice(1).some(
        part => part.trim().length > 0 &&
          ["INSERT", "UPDATE", "DELETE"].some(keyword =>
            part.trim().toUpperCase().startsWith(keyword)
          )
      );
    } else {
      isDML = ["INSERT", "UPDATE", "DELETE"].some(keyword =>
        trimmedQuery.toUpperCase().startsWith(keyword)
      );
    }

    if (isDML) {
      // For DML queries, use POST on the /query/service endpoint.
      try {
        const response = await this.httpClient.post('/query/service', { statement: query });
        return response.data;
      } catch (error) {
        const errorMsg = error.response && error.response.data
          ? JSON.stringify(error.response.data)
          : error.message;
        throw new Error(`Query execution failed: ${errorMsg}`);
      }
    } else {
      // For read-only queries, use GET on the /query/service endpoint.
      try {
        const response = await this.httpClient.get('/query/service', {
          params: { statement: query }
        });
        return response.data;
      } catch (error) {
        const errorMsg = error.response && error.response.data
          ? JSON.stringify(error.response.data)
          : error.message;
        throw new Error(`Query execution failed: ${errorMsg}`);
      }
    }
  }

  /**
   * Executes a SQL++ query in asynchronous mode.
   *
   * This method sends the query using a POST request to `/query/service`.
   * Then, it periodically polls `/query/service/status` to check if the query has completed.
   * Once complete, it retrieves the final results from `/query/service/result`.
   *
   * @param {string} query - The SQL++ query string.
   * @param {number} pollInterval - The interval (in milliseconds) to poll the status endpoint.
   * @returns {Promise<any>} The JSON response data from AsterixDB.
   * @throws {Error} If the asynchronous query fails or times out.
   */
  async executeQueryAsync(query, pollInterval = 1000) {
    try {
      // Submit the query via POST.
      const submitResponse = await this.httpClient.post('/query/service', { statement: query });
      const requestID = submitResponse.data.requestID;
      if (!requestID) {
        throw new Error('No requestID received for asynchronous query.');
      }

      // Poll the status endpoint until the query is complete.
      let statusResponse;
      while (true) {
        // Wait for the specified poll interval.
        await new Promise(resolve => setTimeout(resolve, pollInterval));

        statusResponse = await this.httpClient.get('/query/service/status', {
          params: { requestID }
        });
        const statusData = statusResponse.data;

        // Assuming the status data contains a 'status' field.
        if (statusData.status && statusData.status.toLowerCase() === 'success') {
          break;
        } else if (statusData.status && statusData.status.toLowerCase() === 'fatal') {
          throw new Error(`Asynchronous query failed: ${JSON.stringify(statusData)}`);
        }
        // Continue polling until success.
      }

      // Once successful, fetch the result.
      const resultResponse = await this.httpClient.get('/query/service/result', {
        params: { requestID }
      });
      return resultResponse.data;
    } catch (error) {
      const errorMsg = error.response && error.response.data
        ? JSON.stringify(error.response.data)
        : error.message;
      throw new Error(`Asynchronous query execution failed: ${errorMsg}`);
    }
  }

  /**
   * Performs a generic POST request.
   *
   * @param {string} endpoint - The API endpoint (relative to the base URL).
   * @param {object} data - The data to send in the POST request.
   * @returns {Promise<any>} The JSON response data.
   * @throws {Error} If the POST request fails.
   */
  async post(endpoint, data) {
    try {
      const response = await this.httpClient.post(endpoint, data);
      return response.data;
    } catch (error) {
      const errorMsg = error.response && error.response.data
        ? JSON.stringify(error.response.data)
        : error.message;
      throw new Error(`POST request failed: ${errorMsg}`);
    }
  }
}

module.exports = Connector;
