import React, { useState, useEffect } from 'react';
import { signIn, resetPassword, confirmResetPassword } from 'aws-amplify/auth';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import './Login.css';

const SurveyLogin = () => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [isResetPassword, setIsResetPassword] = useState(false);
  const [isConfirmReset, setIsConfirmReset] = useState(false);
  const [resetCode, setResetCode] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [resetEmail, setResetEmail] = useState('');
  
  const navigate = useNavigate();
  const location = useLocation();
  const { isAuthenticated } = useAuth();

  // Get redirect path from URL parameters
  const searchParams = new URLSearchParams(location.search);
  const redirectPath = searchParams.get('redirect') || '/survey';

  // Redirect to survey if already authenticated
  useEffect(() => {
    if (isAuthenticated) {
      navigate(redirectPath, { replace: true });
    }
  }, [isAuthenticated, navigate, redirectPath]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      const signInResult = await signIn({ username: email, password });
      console.log('Sign in result:', signInResult);
      
      if (signInResult.isSignedIn) {
        console.log('Successfully signed in, navigating to:', redirectPath);
        // Use React Router navigation instead of hard reload
        navigate(redirectPath, { replace: true });
      } else {
        console.error('Sign in completed but user not signed in:', signInResult);
        setError('Sign in completed but session not established. Please try again.');
      }
    } catch (err) {
      console.error('Auth error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleResetPassword = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      await resetPassword({ username: resetEmail });
      setIsConfirmReset(true);
      setError('');
    } catch (err) {
      console.error('Reset password error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleConfirmReset = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      await confirmResetPassword({
        username: resetEmail,
        confirmationCode: resetCode,
        newPassword
      });
      
      // Reset states and show success message
      setIsResetPassword(false);
      setIsConfirmReset(false);
      setError('Password reset successful. Please sign in.');
    } catch (err) {
      console.error('Confirm reset error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-page">
      <div className="login-box">
        <h2>
          {isConfirmReset 
            ? 'Reset Your Password'
            : isResetPassword 
              ? 'Forgot Password'
              : 'Survey Access'}
        </h2>
        
        {isResetPassword ? (
          isConfirmReset ? (
            <form onSubmit={handleConfirmReset}>
              {error && <div className="error-message">{error}</div>}
              <div className="input-group">
                <input
                  type="text"
                  value={resetCode}
                  onChange={(e) => setResetCode(e.target.value)}
                  placeholder="Enter reset code"
                  required
                />
              </div>
              <div className="input-group">
                <input
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  placeholder="New password"
                  required
                />
              </div>
              <button 
                type="submit" 
                className="login-button"
                disabled={loading}
              >
                {loading ? 'Resetting...' : 'Reset Password'}
              </button>
              <button
                type="button"
                className="switch-auth-button"
                onClick={() => {
                  setIsResetPassword(false);
                  setIsConfirmReset(false);
                }}
              >
                Back to Sign In
              </button>
            </form>
          ) : (
            <form onSubmit={handleResetPassword}>
              {error && <div className="error-message">{error}</div>}
              <div className="input-group">
                <input
                  type="email"
                  value={resetEmail}
                  onChange={(e) => setResetEmail(e.target.value)}
                  placeholder="Enter your email"
                  required
                />
              </div>
              <button 
                type="submit" 
                className="login-button"
                disabled={loading}
              >
                {loading ? 'Sending...' : 'Send Reset Code'}
              </button>
              <button
                type="button"
                className="switch-auth-button"
                onClick={() => setIsResetPassword(false)}
              >
                Back to Sign In
              </button>
            </form>
          )
        ) : (
          <form onSubmit={handleSubmit}>
            {error && <div className="error-message">{error}</div>}
            <div className="input-group">
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="Email"
                required
              />
            </div>
            <div className="input-group">
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Password"
                required
              />
            </div>
            <button
              type="button"
              className="forgot-password-button"
              onClick={() => {
                setIsResetPassword(true);
                setResetEmail(email);
              }}
            >
              Forgot Password?
            </button>
            <button 
              type="submit" 
              className="login-button"
              disabled={loading}
            >
              {loading ? 'Signing In...' : 'Access Survey'}
            </button>
          </form>
        )}
      </div>
    </div>
  );
};

export default SurveyLogin; 