/**
 * @fileoverview Contract signing component that provides an interface for viewing,
 * signing, and downloading contracts with signature capture functionality.
 */

import React, { useState, useEffect, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import SignaturePad from 'react-signature-canvas';
import axios from 'axios';
import './SignContract.css';
import { API_ENDPOINTS } from '../../config/api';

/**
 * @typedef {Object} Contract
 * @property {string} client_name - Name of the client
 * @property {string} content - Contract content
 * @property {string} contract_id - Unique contract identifier
 * @property {string} status - Contract status
 */

/**
 * Contract signing component that handles contract viewing and signature collection.
 * Features include:
 * - Contract content display
 * - Digital signature capture
 * - Name confirmation
 * - Contract download
 * - IP address tracking
 * - Signature validation
 * 
 * @returns {React.ReactElement} The rendered contract signing interface
 */
const SignContract = () => {
    const { clientId: urlClientId } = useParams();
    const navigate = useNavigate();
    const [contract, setContract] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [typedName, setTypedName] = useState('');
    const [signed, setSigned] = useState(false);
    const sigPad = useRef({});
    const [isSigningError, setIsSigningError] = useState(false);
    const [isSigning, setIsSigning] = useState(false);
    const [contractId, setContractId] = useState(null);

    /**
     * Fetches contract data when component mounts
     */
    useEffect(() => {
        const fetchContract = async () => {
            try {
                console.log('Fetching contract for client ID:', urlClientId);
                const response = await axios.get(API_ENDPOINTS.CONTRACTS.CONTRACT(urlClientId));
                console.log('Contract response:', response.data);
                
                if (response.data.status === 'success') {
                    setContract(response.data);
                    setContractId(response.data.contract_id);
                } else {
                    setError('Contract not found');
                }
            } catch (error) {
                console.error('Error fetching contract:', error);
                setError(error.response?.data?.detail || error.message);
            } finally {
                setLoading(false);
            }
        };

        if (urlClientId) {
            fetchContract();
        }
    }, [urlClientId]);

    /**
     * Handles contract signing process
     * @async
     * @returns {Promise<void>}
     */
    const handleSign = async () => {
        if (sigPad.current.isEmpty()) {
            setIsSigningError(true);
            return;
        }

        if (!typedName.trim()) {
            setIsSigningError(true);
            alert('Please type your name to confirm agreement');
            return;
        }

        try {
            setIsSigning(true);
            setIsSigningError(false);
            const signatureData = sigPad.current.getTrimmedCanvas().toDataURL('image/png');
            
            console.log('Sending signature data:', {
                client_id: urlClientId,
                contract_id: contractId,
                name: typedName.trim(),
                content: contract.content
            });

            const response = await axios.post(API_ENDPOINTS.CONTRACTS.SIGN, {
                client_id: urlClientId,
                contract_id: contractId,
                signature: signatureData,
                name: typedName.trim(),
                content: contract.content,
                ip_address: await fetch('https://api.ipify.org?format=json')
                    .then(r => r.json())
                    .then(data => data.ip)
            });

            console.log('Sign response:', response.data);

            if (response.data.status === 'success') {
                setSigned(true);
            }
        } catch (error) {
            setIsSigningError(true);
            console.error('Error signing:', error);
            console.error('Error details:', error.response?.data);
            alert(`Failed to sign contract: ${error.response?.data?.detail || 'Unknown error'}`);
        } finally {
            setIsSigning(false);
        }
    };

    /**
     * Downloads the signed contract as PDF
     * @async
     * @returns {Promise<void>}
     */
    const handleDownload = async () => {
        try {
            const response = await axios.get(API_ENDPOINTS.CONTRACTS.DOWNLOAD(urlClientId), {
                responseType: 'blob'
            });
            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', `signed_contract_${contract.client_name}.pdf`);
            document.body.appendChild(link);
            link.click();
            link.remove();
        } catch (error) {
            console.error('Error downloading contract:', error);
            alert('Failed to download contract');
        }
    };

    if (loading) return <div>Loading...</div>;
    if (error) return <div>Error: {error}</div>;

    if (signed) {
        return (
            <div className="signature-container">
                <h2>Contract Signed Successfully!</h2>
                <div className="success-message">
                    <p>Thank you for signing the contract.</p>
                    <p>A copy of the signed contract has been sent to your email.</p>
                </div>
            </div>
        );
    }

    return (
        <div className="signature-container">
            <h2>Sign Contract for {contract.client_name}</h2>
            <div className="contract-content">
                {contract.content}
            </div>
            <div className="signature-section">
                <div className="name-input-section">
                    <label>Please type your full name to confirm agreement:</label>
                    <input
                        type="text"
                        value={typedName}
                        onChange={(e) => setTypedName(e.target.value)}
                        placeholder="Type your full name"
                        className="name-input"
                    />
                </div>
                <p>I, {contract.client_name}, agree to the terms above.</p>
                <SignaturePad
                    ref={sigPad}
                    canvasProps={{
                        className: "signature-pad"
                    }}
                />
            </div>
            <div className="signature-actions">
                <button onClick={() => sigPad.current.clear()}>Clear</button>
                <button 
                    onClick={handleSign}
                    disabled={isSigning}
                >
                    {isSigning ? 'Signing...' : 'Sign Contract'}
                </button>
            </div>
            {isSigningError && (
                <div className="error-message">
                    Failed to save signature. Please try again.
                </div>
            )}
        </div>
    );
};

export default SignContract; 