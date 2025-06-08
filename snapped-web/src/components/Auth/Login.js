import React, { useState } from 'react';
import { signIn, signUp, confirmSignUp, resetPassword, confirmResetPassword } from 'aws-amplify/auth';
import { useNavigate, useLocation } from 'react-router-dom';
import { API_ENDPOINTS } from '../../config/api';
import './Login.css';

const Login = () => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [firstName, setFirstName] = useState('');
  const [lastName, setLastName] = useState('');
  const [birthday, setBirthday] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [isSignUp, setIsSignUp] = useState(false);
  const [confirmPassword, setConfirmPassword] = useState('');
  const [passwordError, setPasswordError] = useState('');
  
  // Search related states
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [selectedItem, setSelectedItem] = useState(null);
  const [isSearching, setIsSearching] = useState(false);
  
  // Add phone state
  const [phone, setPhone] = useState('');
  
  // Add new state for confirmation
  const [isConfirmation, setIsConfirmation] = useState(false);
  const [confirmationCode, setConfirmationCode] = useState('');
  
  // Add state to store employee data temporarily
  const [pendingEmployeeData, setPendingEmployeeData] = useState(null);
  
  // Add new state for password reset
  const [isResetPassword, setIsResetPassword] = useState(false);
  const [isConfirmReset, setIsConfirmReset] = useState(false);
  const [resetCode, setResetCode] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [resetEmail, setResetEmail] = useState('');
  
  const navigate = useNavigate();
  const location = useLocation();

  // Get the redirect path from URL parameters
  const searchParams = new URLSearchParams(location.search);
  const redirectPath = searchParams.get('redirect') || '/lead';

  const handleSearch = async (query) => {
    setSearchQuery(query);
    setIsSearching(true);
    
    if (query.length < 3) {
      setSearchResults([]);
      setIsSearching(false);
      return;
    }

    try {
      console.log('Searching for:', query); // Debug log
      const response = await fetch(API_ENDPOINTS.EMPLOYEES.PARTNER_SEARCH(query));
      
      if (!response.ok) {
        throw new Error(`Search failed: ${response.status}`);
      }
      
      const data = await response.json();
      console.log('Search results:', data); // Debug log
      
      if (data.results && Array.isArray(data.results)) {
        setSearchResults(data.results);
      } else {
        console.error('Unexpected response format:', data);
        setSearchResults([]);
      }
    } catch (err) {
      console.error('Search error:', err);
      setSearchResults([]);
    } finally {
      setIsSearching(false);
    }
  };

  const handleSelectItem = (item) => {
    setSelectedItem(item);
    setSearchQuery(item.name);
    setSearchResults([]);
  };

  const validatePassword = (pass, confirm) => {
    if (!pass) {
      setPasswordError('');
      return false;
    }

    const requirements = [];
    if (!/\d/.test(pass)) requirements.push('number');
    if (!/[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]/.test(pass)) requirements.push('special character');
    if (!/[A-Z]/.test(pass)) requirements.push('uppercase letter');
    if (!/[a-z]/.test(pass)) requirements.push('lowercase letter');
    if (pass.length < 8) requirements.push('minimum 8 characters');

    if (requirements.length > 0) {
      setPasswordError(`Password needs: ${requirements.join(', ')}`);
      return false;
    }

    if (confirm && pass !== confirm) {
      setPasswordError("Passwords don't match");
      return false;
    }

    setPasswordError('');
    return true;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (isSignUp && !validatePassword(password, confirmPassword)) {
      return;
    }
    setLoading(true);
    setError('');

    try {
      if (isSignUp) {
        // Generate the user_id from initials and birthday
        const first_initial = firstName[0].toLowerCase();
        const last_initial = lastName[0].toLowerCase();
        const [year, month, day] = birthday.split('-');
        const formatted_date = `${month}${day}${year}`;  // Rearrange to MMDDYYYY
        const user_id = `${first_initial}${last_initial}${formatted_date}`;

        // Prepare employee data
        const employeeData = {
          email,
          user_id,
          first_name: firstName,
          last_name: lastName,
          date_of_birth: birthday,
          phone_number: phone.startsWith('+') ? phone : `+1${phone}`,
          company_id: selectedItem?.id  // This is the actual partner ID from MongoDB
        };

        console.log('Employee data before sending:', employeeData); // Debug log

        // Store employee data for later
        setPendingEmployeeData(employeeData);

        // Sign up with Cognito
        await signUp({
          username: email,
          password,
          options: {
            userAttributes: {
              email,
              given_name: firstName,
              family_name: lastName,
              birthdate: birthday,
              'custom:UserID': user_id,
              phone_number: phone.startsWith('+') ? phone : `+1${phone}`,
            },
            autoSignIn: true
          }
        });

        // Show confirmation screen
        setIsConfirmation(true);
        return;
      } else {
        // Sign in using email as username
        const signInResult = await signIn({ username: email, password });
        console.log('Sign in result:', signInResult);
        
        if (signInResult.isSignedIn) {
          console.log('Successfully signed in, navigating to:', redirectPath);
          // Force a page reload to ensure AWS credentials are properly set
          window.location.href = redirectPath;
        } else {
          console.error('Sign in completed but user not signed in:', signInResult);
          setError('Sign in completed but session not established. Please try again.');
        }
      }
    } catch (err) {
      console.error('Auth error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleConfirmSignUp = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      // Confirm signup
      await confirmSignUp({
        username: email,
        confirmationCode: confirmationCode
      });

      // Create employee record in MongoDB
      if (pendingEmployeeData) {
        const response = await fetch(API_ENDPOINTS.EMPLOYEES.SIGNUP, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(pendingEmployeeData)
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(`Failed to create employee record: ${JSON.stringify(errorData)}`);
        }
      }

      // Clear the pending data
      setPendingEmployeeData(null);
      
      // Sign in the user
      const signInResult = await signIn({ username: email, password });
      console.log('Sign in successful:', signInResult);
      
      // Force a page reload to ensure AWS credentials are properly set
      window.location.href = redirectPath;
    } catch (err) {
      console.error('Confirmation error:', err);
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
    if (!validatePassword(newPassword)) {
      return;
    }
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
              : isConfirmation 
                ? 'Confirm Your Email' 
                : (isSignUp ? 'Create Account' : 'Welcome Back')}
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
                  onChange={(e) => {
                    setNewPassword(e.target.value);
                    validatePassword(e.target.value);
                  }}
                  placeholder="New password"
                  required
                />
              </div>
              {passwordError && (
                <div className="error-message password-requirements">
                  {passwordError}
                </div>
              )}
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
        ) : isConfirmation ? (
          <form onSubmit={handleConfirmSignUp}>
            {error && <div className="error-message">{error}</div>}
            <div className="input-group">
              <input
                type="text"
                value={confirmationCode}
                onChange={(e) => setConfirmationCode(e.target.value)}
                placeholder="Enter confirmation code"
                required
              />
            </div>
            <button 
              type="submit" 
              className="login-button"
              disabled={loading}
            >
              {loading ? 'Confirming...' : 'Confirm Email'}
            </button>
          </form>
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
            {isSignUp && (
              <>
                <div className="input-group">
                  <input
                    type="text"
                    value={firstName}
                    onChange={(e) => setFirstName(e.target.value)}
                    placeholder="First Name"
                    required
                  />
                </div>
                <div className="input-group">
                  <input
                    type="text"
                    value={lastName}
                    onChange={(e) => setLastName(e.target.value)}
                    placeholder="Last Name"
                    required
                  />
                </div>
                <div className="input-group">
                  <input
                    type="date"
                    value={birthday}
                    onChange={(e) => setBirthday(e.target.value)}
                    placeholder="Birthday"
                    required
                  />
                </div>
                
                {/* Search Section - Now inside isSignUp condition */}
                <div className="search-section">
                  <div className="input-group">
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(e) => handleSearch(e.target.value)}
                      placeholder="Search Partner..."
                      className="form-input search-input"
                    />
                  </div>
                  
                  {searchResults.length > 0 && (
                    <div className="search-results">
                      {searchResults.map((result) => (
                        <div
                          key={result.id}
                          className="search-result-item"
                          onClick={() => handleSelectItem(result)}
                        >
                          {result.name}
                        </div>
                      ))}
                    </div>
                  )}
                  
                  {selectedItem && (
                    <div className="selected-item">
                      <span>Selected: {selectedItem.name}</span>
                      <button 
                        className="remove-selection"
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedItem(null);
                        }}
                        type="button"
                      >
                        ×
                      </button>
                    </div>
                  )}
                </div>
                <div className="input-group">
                  <input
                    type="tel"
                    value={phone}
                    onChange={(e) => {
                      // Only allow numbers and plus sign
                      const cleaned = e.target.value.replace(/[^\d+]/g, '');
                      setPhone(cleaned);
                    }}
                    placeholder="Phone Number (e.g., +1234567890)"
                    required
                  />
                </div>
                <div className="input-group">
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => {
                      setPassword(e.target.value);
                      validatePassword(e.target.value, confirmPassword);
                    }}
                    placeholder="Password"
                    required
                  />
                </div>
                <div className="input-group">
                  <input
                    type="password"
                    value={confirmPassword}
                    onChange={(e) => {
                      setConfirmPassword(e.target.value);
                      validatePassword(password, e.target.value);
                    }}
                    placeholder="Confirm Password"
                    required
                  />
                </div>
                {passwordError && (
                  <div className="error-message password-requirements">
                    {passwordError}
                  </div>
                )}
              </>
            )}
            {!isSignUp && (
              <div className="input-group">
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Password"
                  required
                />
              </div>
            )}
            {!isSignUp && !isConfirmation && (
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
            )}
            <button 
              type="submit" 
              className="login-button"
              disabled={loading}
            >
              {loading ? 'Processing...' : (isSignUp ? 'Sign Up' : 'Sign In')}
            </button>
            <button
              type="button"
              className="switch-auth-button"
              onClick={() => setIsSignUp(!isSignUp)}
            >
              {isSignUp ? 'Already have an account? Sign In' : 'Need an account? Sign Up'}
            </button>
          </form>
        )}
      </div>
    </div>
  );
};

export default Login; 

