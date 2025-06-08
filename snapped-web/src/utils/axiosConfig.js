/**
 * @fileoverview Axios configuration with request and response interceptors.
 * Sets up a custom axios instance with authentication and error handling.
 */

import axios from 'axios';

/**
 * Custom axios instance with interceptors for authentication and error handling
 * @type {import('axios').AxiosInstance}
 */
const axiosInstance = axios.create();

/**
 * Request interceptor that adds authentication token to requests
 * Retrieves token from localStorage and adds it to the Authorization header
 */
axiosInstance.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

/**
 * Response interceptor that handles authentication errors
 * Redirects to login page on 401 unauthorized responses
 */
axiosInstance.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

export default axiosInstance; 