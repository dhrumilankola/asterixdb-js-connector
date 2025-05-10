const axios = require('axios');

/**
 * Connector class to handle HTTP communication with AsterixDB.
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
 * This method sends the query using a POST request to `/query/service` with mode "async"
 * and additional parameters. It expects an initial response with status "running" and a "handle"
 * which is a URL. It then polls that URL until the query is complete. When the status returns
 * "success" with a new handle, it uses that handle as the URL to fetch the final result.
 *
 * @param {string} query - The SQL++ query string.
 * @param {number} pollInterval - The interval (in milliseconds) between status polls.
 * @param {number} maxAttempts - Maximum number of polling attempts.
 * @returns {Promise<any>} The final query result.
 * @throws {Error} If the asynchronous query fails or times out.
 */
async executeQueryAsync(query, pollInterval = 1000, maxAttempts = 10) {
    try {
      // Prepare payload
      const payload = {
        statement: query.trim(),
        mode: "async",
        pretty: false
      };
      console.debug("Submitting async query with payload:", JSON.stringify(payload, null, 2));
  
      // Submit query in async mode.
      const submitResponse = await this.httpClient.post('/query/service', payload);
      console.debug("Async submit response status:", submitResponse.status);
      console.debug("Async submit response data:", submitResponse.data);
      const initialResponse = submitResponse.data;
  
      // Verify the initial async response.
      if (initialResponse.status !== "running" || !initialResponse.handle) {
        console.error("Unexpected async response:", initialResponse);
        throw new Error("Invalid async query response: " + JSON.stringify(initialResponse));
      }
      const statusUrl = initialResponse.handle;
      console.debug("Received initial async handle (status URL):", statusUrl);
  
      let attempts = 0;
      let statusResponse;
      while (attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, pollInterval));
        console.debug(`Polling async status (attempt ${attempts + 1}/${maxAttempts}) at URL: ${statusUrl}...`);
  
        // Use the handle URL directly.
        statusResponse = await this.httpClient.get(statusUrl);
        console.debug("Status response status code:", statusResponse.status);
        console.debug("Status response data:", statusResponse.data);
        const statusData = statusResponse.data;
  
        // If status indicates success and provides a new handle, use it for results.
        if (statusData.status && statusData.status.toLowerCase() === "success" && statusData.handle) {
          console.debug("Async query successful; new result handle received:", statusData.handle);
          // Use the new handle URL directly.
          const resultResponse = await this.httpClient.get(statusData.handle);
          console.debug("Result response data:", resultResponse.data);
          return resultResponse.data;
        } else if (statusData.status && 
                   (statusData.status.toLowerCase() === "failed" || statusData.status.toLowerCase() === "fatal")) {
          console.error("Async query failed with status:", statusData.status, "and data:", statusData);
          throw new Error("Asynchronous query failed: " + JSON.stringify(statusData));
        }
        attempts++;
      }
      throw new Error("Asynchronous query did not complete within the expected time.");
    } catch (error) {
      if (error.response) {
        console.error("Error response status:", error.response.status);
        console.error("Error response data:", error.response.data);
      }
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
