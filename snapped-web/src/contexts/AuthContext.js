/**
 * @fileoverview Authentication context provider for managing user authentication state.
 * Provides authentication functionality using AWS Amplify Cognito service.
 */

import React, { createContext, useState, useContext, useEffect } from 'react';
import { Amplify } from 'aws-amplify';
import { signOut, getCurrentUser, fetchAuthSession } from 'aws-amplify/auth';
import { Hub } from 'aws-amplify/utils';

/**
 * @typedef {Object} AwsConfig
 * @property {Object} Auth - Authentication configuration
 * @property {Object} Auth.Cognito - Cognito-specific configuration
 * @property {string} Auth.Cognito.userPoolId - Cognito user pool ID
 * @property {string} Auth.Cognito.userPoolClientId - Cognito client ID
 * @property {Object} Auth.Cognito.loginWith - Login configuration
 * @property {Object} Auth.Cognito.loginWith.oauth - OAuth configuration
 */
const awsConfig = {
  Auth: {
    Cognito: {
      userPoolId: 'us-east-2_iIfwSsdCU',
      userPoolClientId: '1rv7iijlcgv4cortina322ntri',
      loginWith: {
        oauth: {
          domain: 'us-east-2iifwssdcu.auth.us-east-2.amazoncognito.com/login',
          scope: ['email', 'openid', 'phone'],
          redirectSignIn: 'https://d84l1y8p4kdic.cloudfront.net',
          redirectSignOut: 'https://d84l1y8p4kdic.cloudfront.net',
          responseType: 'code'
        }
      }
    }
  }
};

Amplify.configure(awsConfig);

/**
 * @typedef {Object} AuthContextType
 * @property {Object|null} user - Current authenticated user object
 * @property {boolean} loading - Loading state of authentication
 * @property {boolean} isAuthenticated - Whether a user is authenticated
 * @property {Function} signOut - Function to sign out the current user
 * @property {Function} getAccessToken - Function to get the current access token
 * @property {Function} checkUser - Function to check and update the current user
 */

/**
 * React context for authentication state and functions
 * @type {React.Context<AuthContextType>}
 */
const AuthContext = createContext({});

/**
 * Authentication provider component that manages auth state and provides auth functions
 * 
 * @component
 * @param {Object} props - Component props
 * @param {React.ReactNode} props.children - Child components to render
 * @returns {React.ReactElement} The rendered AuthProvider component
 */
export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    checkUser();

    /**
     * Event listener for auth state changes
     * @param {Object} data - Event data
     * @param {Object} data.payload - Event payload
     */
    const listener = (data) => {
      const { payload } = data;
      if (payload.event === 'signIn') {
        checkUser();
      }
      if (payload.event === 'signOut') {
        setUser(null);
      }
    };

    const hubListener = Hub.listen('auth', listener);
    return () => hubListener();
  }, []);

  /**
   * Checks and updates the current authenticated user
   * @async
   * @function checkUser
   * @returns {Promise<void>}
   */
  async function checkUser() {
    try {
      const currentUser = await getCurrentUser();
      setUser(currentUser);
    } catch (err) {
      setUser(null);
    } finally {
      setLoading(false);
    }
  }

  /**
   * Gets the current user's access token
   * @async
   * @function getAccessToken
   * @returns {Promise<string|null>} The access token or null if not available
   */
  const getAccessToken = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      return tokens.idToken.toString();
    } catch (error) {
      console.error('Error getting access token:', error);
      return null;
    }
  };

  /**
   * Signs out the current user
   * @async
   * @function handleSignOut
   * @returns {Promise<void>}
   */
  const handleSignOut = async () => {
    try {
      await signOut();
      setUser(null);
    } catch (error) {
      console.error('Error signing out:', error);
    }
  };

  const value = {
    user,
    loading,
    isAuthenticated: !!user,
    signOut: handleSignOut,
    getAccessToken,
    checkUser
  };

  return (
    <AuthContext.Provider value={value}>
      {!loading && children}
    </AuthContext.Provider>
  );
};

/**
 * Custom hook to access the auth context
 * @function useAuth
 * @returns {AuthContextType} The auth context value
 */
export const useAuth = () => {
  return useContext(AuthContext);
}; 