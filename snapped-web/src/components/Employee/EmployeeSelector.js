/**
 * @fileoverview Employee selector component for choosing and displaying employee information.
 * Provides a searchable dropdown interface with detailed employee information.
 */

import React, { useState, useEffect } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faUser, faChevronDown, faSearch, faSpinner } from '@fortawesome/free-solid-svg-icons';
import { API_ENDPOINTS } from '../../config/api';
import { useAuth } from '../../contexts/AuthContext';
import { fetchAuthSession } from 'aws-amplify/auth';
import axios from 'axios';
import './EmployeeSelector.css';

/**
 * @typedef {Object} Employee
 * @property {string} user_id - Unique identifier for the employee
 * @property {string} first_name - Employee's first name
 * @property {string} last_name - Employee's last name
 * @property {string} email - Employee's email address
 * @property {string} [department] - Employee's department
 * @property {number} [rate] - Employee's hourly rate
 */

/**
 * Employee selector component for managing employee selection and display.
 * Features:
 * - Searchable employee dropdown
 * - Detailed employee information display
 * - Loading state management
 * - Error handling
 * - Department and rate information
 * - Real-time search filtering
 * 
 * @param {Object} props - Component properties
 * @param {Function} props.onEmployeeSelect - Handler for employee selection
 * @param {Employee} props.selectedEmployee - Currently selected employee
 * @returns {React.ReactElement} The employee selector interface
 */
const EmployeeSelector = ({ onEmployeeSelect, selectedEmployee }) => {
  const { user } = useAuth();
  const [employees, setEmployees] = useState([]);
  const [filteredEmployees, setFilteredEmployees] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchEmployees();
  }, []);

  useEffect(() => {
    if (searchTerm) {
      const filtered = employees.filter(employee =>
        `${employee.first_name} ${employee.last_name}`.toLowerCase().includes(searchTerm.toLowerCase()) ||
        employee.email.toLowerCase().includes(searchTerm.toLowerCase()) ||
        employee.user_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
        (employee.department && employee.department.toLowerCase().includes(searchTerm.toLowerCase()))
      );
      setFilteredEmployees(filtered);
    } else {
      setFilteredEmployees(employees);
    }
  }, [searchTerm, employees]);

  const fetchEmployees = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      const response = await axios.get(API_ENDPOINTS.EMPLOYEES.LIST, {
        headers: {
          Authorization: `Bearer ${token}`
        }
      });
      
      setEmployees(response.data || []);
    } catch (error) {
      console.error('Error fetching employees:', error);
      setError('Failed to load employees. ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const handleEmployeeSelect = (employee) => {
    onEmployeeSelect(employee);
    setIsDropdownOpen(false);
    setSearchTerm('');
  };

  const handleSearchChange = (e) => {
    setSearchTerm(e.target.value);
  };

  return (
    <div className="employee-selector">
      <div className="selector-header">
        <FontAwesomeIcon icon={faUser} className="selector-icon" />
        <span className="selector-label">Select Employee</span>
      </div>

      <div className="selector-dropdown">
        <div 
          className="selector-input"
          onClick={() => setIsDropdownOpen(!isDropdownOpen)}
        >
          <span className={`selected-employee ${!selectedEmployee ? 'placeholder' : ''}`}>
            {selectedEmployee 
              ? `${selectedEmployee.first_name} ${selectedEmployee.last_name}` 
              : 'Choose an employee...'
            }
          </span>
          <FontAwesomeIcon 
            icon={faChevronDown} 
            className="dropdown-icon"
            style={{ transform: isDropdownOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}
          />
        </div>

        {isDropdownOpen && (
          <div className="dropdown-content">
            <div className="search-box">
              <FontAwesomeIcon icon={faSearch} className="search-icon" />
              <input
                type="text"
                placeholder="Search employees..."
                value={searchTerm}
                onChange={handleSearchChange}
                autoFocus
              />
            </div>

            <div className="employee-list">
              {loading && (
                <div className="loading-item">
                  <FontAwesomeIcon icon={faSpinner} spin />
                  Loading employees...
                </div>
              )}

              {error && (
                <div className="error-item">
                  {error}
                </div>
              )}

              {!loading && !error && filteredEmployees.length === 0 && (
                <div className="no-results">
                  No employees found
                </div>
              )}

              {!loading && !error && filteredEmployees.map((employee) => (
                <div
                  key={employee.user_id}
                  className={`employee-item ${selectedEmployee?.user_id === employee.user_id ? 'selected' : ''}`}
                  onClick={() => handleEmployeeSelect(employee)}
                >
                  <div className="employee-info">
                    <div className="employee-name">
                      {employee.first_name} {employee.last_name}
                    </div>
                    <div className="employee-details">
                      <span className="employee-id">{employee.user_id}</span>
                      {employee.department && (
                        <>
                          <span> • </span>
                          <span className="employee-department">{employee.department}</span>
                        </>
                      )}
                      {employee.rate && (
                        <>
                          <span> • </span>
                          <span className="employee-rate">${employee.rate}/hr</span>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {selectedEmployee && (
        <div className="selected-employee-info">
          <div className="info-row">
            <span className="info-label">ID:</span>
            <span className="info-value">{selectedEmployee.user_id}</span>
          </div>
          <div className="info-row">
            <span className="info-label">Email:</span>
            <span className="info-value">{selectedEmployee.email}</span>
          </div>
          {selectedEmployee.department && (
            <div className="info-row">
              <span className="info-label">Dept:</span>
              <span className="info-value">{selectedEmployee.department}</span>
            </div>
          )}
          {selectedEmployee.rate && (
            <div className="info-row">
              <span className="info-label">Rate:</span>
              <span className="info-value">${selectedEmployee.rate}/hr</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default EmployeeSelector; 