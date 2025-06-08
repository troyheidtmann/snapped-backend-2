import React, { useState } from 'react';
import '../CallForm/CallForm.css';

const PayeeForm = () => {
    const [formData, setFormData] = useState({
        name: '',
        company_name: '',
        legal_first_name: '',
        legal_last_name: '',
        primary_email: '',
        billing_address: '',
        phone_number: '',
        tax_id: '',
        routing_number: '',
        account_number: '',
        bank: ''
    });

    const [errors, setErrors] = useState({});
    const [submitStatus, setSubmitStatus] = useState(null);

    const handleChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({
            ...prev,
            [name]: value
        }));
        // Clear error when user starts typing
        if (errors[name]) {
            setErrors(prev => ({
                ...prev,
                [name]: null
            }));
        }
    };

    const validateForm = () => {
        const newErrors = {};
        
        // Required fields
        const requiredFields = [
            'name', 'legal_first_name', 'legal_last_name', 
            'primary_email', 'billing_address', 'phone_number',
            'tax_id', 'routing_number', 'account_number', 'bank'
        ];
        
        requiredFields.forEach(field => {
            if (!formData[field]?.trim()) {
                newErrors[field] = 'This field is required';
            }
        });

        // Email validation
        if (formData.primary_email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.primary_email)) {
            newErrors.primary_email = 'Please enter a valid email address';
        }

        // Phone validation
        if (formData.phone_number && !/^\+?[\d\s-()]+$/.test(formData.phone_number)) {
            newErrors.phone_number = 'Please enter a valid phone number';
        }

        setErrors(newErrors);
        return Object.keys(newErrors).length === 0;
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (!validateForm()) return;

        try {
            setSubmitStatus('submitting');
            
            // First save the payee
            const saveResponse = await fetch('/api/payments/payee', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(formData)
            });
            
            if (saveResponse.ok) {
                // Then trigger QuickBooks sync
                await fetch('/api/payments/quickbooks/sync', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ payee_email: formData.primary_email })
                });
                
                setSubmitStatus('success');
                // Clear form
                setFormData({
                    name: '',
                    company_name: '',
                    legal_first_name: '',
                    legal_last_name: '',
                    primary_email: '',
                    billing_address: '',
                    phone_number: '',
                    tax_id: '',
                    routing_number: '',
                    account_number: '',
                    bank: ''
                });
            }
        } catch (error) {
            console.error('Error:', error);
            setSubmitStatus('error');
        }
    };

    return (
        <div className="call-form-page call-form">
            <div className="snapped-call-form__container">
                <h1 className="snapped-call-form__title">Payee Information</h1>
                
                <form onSubmit={handleSubmit}>
                    <div className="snapped-call-form__section">
                        <h2 className="snapped-call-form__section-heading">Basic Information</h2>
                        
                        <input
                            type="text"
                            name="name"
                            placeholder="Name *"
                            value={formData.name}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.name ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.name && <span className="snapped-call-form__error">{errors.name}</span>}

                        <input
                            type="text"
                            name="company_name"
                            placeholder="Company Name"
                            value={formData.company_name}
                            onChange={handleChange}
                            className="snapped-call-form__input"
                        />

                        <input
                            type="text"
                            name="legal_first_name"
                            placeholder="Legal First Name *"
                            value={formData.legal_first_name}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.legal_first_name ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.legal_first_name && <span className="snapped-call-form__error">{errors.legal_first_name}</span>}

                        <input
                            type="text"
                            name="legal_last_name"
                            placeholder="Legal Last Name *"
                            value={formData.legal_last_name}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.legal_last_name ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.legal_last_name && <span className="snapped-call-form__error">{errors.legal_last_name}</span>}
                    </div>

                    <div className="snapped-call-form__section">
                        <h2 className="snapped-call-form__section-heading">Contact Information</h2>
                        
                        <input
                            type="email"
                            name="primary_email"
                            placeholder="Primary Email *"
                            value={formData.primary_email}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.primary_email ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.primary_email && <span className="snapped-call-form__error">{errors.primary_email}</span>}

                        <input
                            type="text"
                            name="billing_address"
                            placeholder="Billing Address *"
                            value={formData.billing_address}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.billing_address ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.billing_address && <span className="snapped-call-form__error">{errors.billing_address}</span>}

                        <input
                            type="tel"
                            name="phone_number"
                            placeholder="Phone Number *"
                            value={formData.phone_number}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.phone_number ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.phone_number && <span className="snapped-call-form__error">{errors.phone_number}</span>}
                    </div>

                    <div className="snapped-call-form__section">
                        <h2 className="snapped-call-form__section-heading">Payment Information</h2>
                        
                        <input
                            type="text"
                            name="tax_id"
                            placeholder="Tax ID *"
                            value={formData.tax_id}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.tax_id ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.tax_id && <span className="snapped-call-form__error">{errors.tax_id}</span>}

                        <input
                            type="text"
                            name="routing_number"
                            placeholder="Routing Number *"
                            value={formData.routing_number}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.routing_number ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.routing_number && <span className="snapped-call-form__error">{errors.routing_number}</span>}

                        <input
                            type="text"
                            name="account_number"
                            placeholder="Account Number *"
                            value={formData.account_number}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.account_number ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.account_number && <span className="snapped-call-form__error">{errors.account_number}</span>}

                        <input
                            type="text"
                            name="bank"
                            placeholder="Bank Name *"
                            value={formData.bank}
                            onChange={handleChange}
                            className={`snapped-call-form__input ${errors.bank ? 'snapped-call-form__input--error' : ''}`}
                        />
                        {errors.bank && <span className="snapped-call-form__error">{errors.bank}</span>}
                    </div>

                    {errors.submit && (
                        <div className="snapped-call-form__error-message">{errors.submit}</div>
                    )}

                    <button 
                        type="submit" 
                        className="snapped-call-form__submit-button"
                        disabled={submitStatus === 'submitting'}
                    >
                        {submitStatus === 'submitting' ? 'Submitting...' : 'Submit'}
                    </button>

                    {submitStatus === 'success' && (
                        <div style={{color: 'green', textAlign: 'center', marginTop: '20px'}}>
                            Form submitted successfully!
                        </div>
                    )}
                </form>
            </div>
        </div>
    );
};

export default PayeeForm; 