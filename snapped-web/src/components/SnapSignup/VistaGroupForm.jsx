import React, { useState } from 'react';
import '../../styles/components/VistaGroupForm.css';
import { COUNTRIES } from './countries';
import { TIMEZONES } from './timezones';

const VistaGroupForm = () => {
  const [formData, setFormData] = useState({
    fullName: '',
    type: 'CLIENT',
    timezone: '',
    maxUsers: 1,
    country: '',
    maxProfiles: 1
  });

  const [modalVisible, setModalVisible] = useState(false);
  const [dynamicLink, setDynamicLink] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    try {
      const response = await fetch('https://hook.us2.make.com/7nl3muuf5ltpvinggj8r1tbndvacms2c', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(formData)
      });
      
      const responseBody = await response.text();
      console.log('Response received:', responseBody);

      if (responseBody.includes('vistasocial.com/connect/')) {
        window.location.href = responseBody.trim();
      } else {
        console.error('Invalid response format:', responseBody);
        alert('Failed to get connection link. Please try again.');
      }
    } catch (error) {
      console.error('Error:', error);
      alert('Failed to get profiles. Please try again.');
    }
  };

  const handleContinue = () => {
    console.log('Attempting to redirect to:', dynamicLink);
    if (dynamicLink && dynamicLink.includes('vistasocial.com/connect/')) {
      window.open(dynamicLink, '_blank');
    } else {
      console.error('Invalid link format:', dynamicLink);
    }
  };

  const handleChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  return (
    <div className="vista-container">
      <h2 className="vista-title">Snapchat Connect</h2>
      
      <form onSubmit={handleSubmit}>
        <div>
          <label className="vista-label">Full Name</label>
          <input
            type="text"
            name="fullName"
            value={formData.fullName}
            onChange={handleChange}
            className="vista-input"
            required
          />
        </div>

        <div>
          <label className="vista-label">Timezone</label>
          <select
            name="timezone"
            value={formData.timezone}
            onChange={handleChange}
            className="vista-select optional"
          >
            <option value="">Select Timezone</option>
            {Object.entries(TIMEZONES).map(([group, timezones]) => (
              <optgroup key={group} label={group}>
                {timezones.map((timezone) => (
                  <option key={timezone} value={timezone}>
                    {timezone}
                  </option>
                ))}
              </optgroup>
            ))}
          </select>
        </div>

        <div>
          <label className="vista-label">Country</label>
          <select
            name="country"
            value={formData.country}
            onChange={handleChange}
            className="vista-select optional"
          >
            <option value="">Select Country</option>
            {Object.entries(COUNTRIES).map(([group, countries]) => (
              <optgroup key={group} label={group}>
                {countries.map((country) => (
                  <option key={country.id} value={country.id}>
                    {country.name}
                  </option>
                ))}
              </optgroup>
            ))}
          </select>
        </div>

        <button type="submit" className="vista-button">Create Group</button>
      </form>

      {modalVisible && (
        <div className="vista-modal">
          <div className="vista-modal-content">
            <p>Now connect your account.</p>
            {dynamicLink && (
              <button onClick={handleContinue} className="vista-button">Continue</button>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default VistaGroupForm; 