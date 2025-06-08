import React, { useState, useEffect, useRef } from 'react';
import SignaturePad from 'react-signature-canvas';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';

const ContractModal = ({ isOpen, onClose, client, onSave }) => {
  const [content, setContent] = useState('');
  const [status, setStatus] = useState('draft');
  const [isSaving, setIsSaving] = useState(false);
  const [showSignaturePad, setShowSignaturePad] = useState(false);
  const [contractId, setContractId] = useState(null);
  const [isContractSigned, setIsContractSigned] = useState(false);
  const sigPad = useRef({});
  const [isSigningError, setIsSigningError] = useState(false);
  const [isSigning, setIsSigning] = useState(false);
  const [versions, setVersions] = useState([]);
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [repName, setRepName] = useState('');

  useEffect(() => {
    const fetchVersions = async () => {
      if (client) {
        try {
          // Use the client ID from your data structure
          const clientId = client.client_id || "jm" + client.DOB?.replace(/-/g, "");
          const response = await axios.get(API_ENDPOINTS.CONTRACTS.VERSIONS(clientId));
          
          console.log('Versions response:', response.data); // Debug log
          
          if (response.data.status === 'success') {
            setVersions(response.data.versions || []);
            // If there are versions, set the latest content
            if (response.data.versions && response.data.versions.length > 0) {
              setContent(response.data.versions[0].content);
            }

            // Check if contract exists and has signatures
            const contractResponse = await axios.get(API_ENDPOINTS.CONTRACTS.CONTRACT(clientId));
            console.log('Contract response:', contractResponse.data); // Debug log
            
            if (contractResponse.data.status === 'success') {
              const contract = contractResponse.data;
              const hasSig = !!(contract.client_signature || contract.representative_signature);
              console.log('Has signature:', hasSig); // Debug log
              setIsContractSigned(hasSig);
              setStatus(contract.contract_status || 'draft');
              if (contract.contract_id) {
                setContractId(contract.contract_id);
              }
            }
          }
        } catch (error) {
          console.error('Error fetching contract data:', error);
        }
      }
    };

    if (isOpen) {
      fetchVersions();
    }
  }, [isOpen, client]);

  useEffect(() => {
    if (isOpen) {
      const fetchVersions = async () => {
        if (client) {
          try {
            // First get the template (draft version)
            let template = localStorage.getItem('contractTemplate') || getDefaultTemplate();
            template = template.replace('{{CLIENT_NAME}}', `${client.First_Legal_Name} ${client.Last_Legal_Name}`);
            
            // Then get any saved versions
            const versionsResponse = await axios.get(API_ENDPOINTS.CONTRACTS.VERSIONS(client._id || client.id));
            if (versionsResponse.data.status === 'success') {
              const newVersions = versionsResponse.data.versions || [];
              setVersions([
                { content: template, timestamp: 'draft', edited_by: 'system' },
                ...newVersions
              ]);
              
              // If there are saved versions, show the latest, otherwise show draft
              if (newVersions.length > 0) {
                setContent(newVersions[0].content);
              } else {
                setContent(template);
              }
              setSelectedVersion(null);
            }
          } catch (error) {
            console.error('Error fetching versions:', error);
          }
        }
      };

      fetchVersions();
    }
  }, [isOpen, client]);

  /**
   * Saves the current contract as a draft
   * @async
   * @returns {Promise<void>}
   */
  const handleSaveDraft = async () => {
    try {
      setIsSaving(true);
      
      const response = await axios.post(API_ENDPOINTS.CONTRACTS.SAVE_DRAFT, {
        client_id: client._id || client.id,
        content: content
      });

      if (response.data.status === 'success') {
        // Fetch fresh versions after saving
        const versionsResponse = await axios.get(API_ENDPOINTS.CONTRACTS.VERSIONS(client._id || client.id));
        if (versionsResponse.data.status === 'success') {
          const newVersions = versionsResponse.data.versions || [];
          setVersions(newVersions);
          // Set content to the newest version and select it
          if (newVersions.length > 0) {
            const newestVersion = newVersions[0];
            setContent(newestVersion.content);
            setSelectedVersion(newestVersion.version);
          }
        }
      }
    } catch (error) {
      console.error('Error saving draft:', error);
    } finally {
      setIsSaving(false);
    }
  };

  /**
   * Sends the contract to the client for signature
   * @async
   * @returns {Promise<void>}
   */
  const handleSendToClient = async () => {
    try {
        setIsSaving(true);
        console.log('Sending contract with data:', {  // Debug log
            contract_id: contractId,
            client_id: client._id || client.id,
            content: content
        });

        const response = await axios.post(API_ENDPOINTS.CONTRACTS.SEND_TO_CLIENT, {
            contract_id: contractId,
            client_id: client._id || client.id,
            content: content
        });

        console.log('Send response:', response.data);  // Debug log

        if (response.data.status === 'success') {
            setStatus(response.data.contract_status);
            alert('Contract sent to client successfully!');
            onClose();
        }
    } catch (error) {
        console.error('Error sending contract:', error);
        console.error('Error details:', error.response?.data);  // Debug log
        alert(`Failed to send contract: ${error.response?.data?.detail || error.message}`);
    } finally {
        setIsSaving(false);
    }
  };

  /**
   * Initiates the representative signing process
   * @async
   * @returns {Promise<void>}
   */
  const handleRepSign = async () => {
    try {
        // First save the contract to get an ID
        const saveResponse = await axios.post(API_ENDPOINTS.CONTRACTS.SAVE_DRAFT, {
            client_id: client._id || client.id,
            content: content
        });

        if (saveResponse.data.status === 'success') {
            // Store the contract ID
            setContractId(saveResponse.data.contract_id);
            // Then show signature pad
            setShowSignaturePad(true);
        }
    } catch (error) {
        console.error('Error preparing for signature:', error);
        alert('Failed to prepare contract for signing');
    }
  };

  /**
   * Handles the signature completion process for both client and representative
   * @async
   * @returns {Promise<void>}
   */
  const handleSign = async () => {
    if (sigPad.current.isEmpty()) {
        setIsSigningError(true);
        return;
    }

    try {
        setIsSigning(true);
        setIsSigningError(false);
        const signatureData = sigPad.current.getTrimmedCanvas().toDataURL('image/png');
        const clientId = client.client_id || "jm" + client.DOB?.replace(/-/g, "");
        
        // If signing as representative
        if (showSignaturePad) {
            const response = await axios.post(API_ENDPOINTS.CONTRACTS.SIGN_REP, {
                contract_id: contractId,
                signature: signatureData,
                name: repName,
                timestamp: new Date().toISOString(),
                ip_address: await fetch('https://api.ipify.org?format=json').then(r => r.json()).then(data => data.ip)
            });

            if (response.data.status === 'success') {
                alert('Contract signed successfully as representative');
                setShowSignaturePad(false);
                setIsContractSigned(true);
                setStatus('rep_signed');
                
                // Refresh contract data after signing
                const contractResponse = await axios.get(API_ENDPOINTS.CONTRACTS.CONTRACT(clientId));
                if (contractResponse.data.status === 'success') {
                    const contract = contractResponse.data;
                    setIsContractSigned(!!(contract.client_signature || contract.representative_signature));
                    setStatus(contract.status || 'draft');
                }
                
                // Refresh versions
                const versionsResponse = await axios.get(API_ENDPOINTS.CONTRACTS.VERSIONS(clientId));
                if (versionsResponse.data.status === 'success') {
                    setVersions(versionsResponse.data.versions || []);
                }
            }
        } else {
            // Client signing logic remains the same
            const response = await axios.post(API_ENDPOINTS.CONTRACTS.SIGN, {
                client_id: clientId,
                content: content,
                signature: signatureData,
                name: `${client.First_Legal_Name} ${client.Last_Legal_Name}`,
                timestamp: new Date().toISOString(),
                ip_address: await fetch('https://api.ipify.org?format=json').then(r => r.json()).then(data => data.ip)
            });

            if (response.data.status === 'success') {
                setStatus('signed');
                setIsContractSigned(true);
                setContractId(response.data.contract_id);
                onClose();
            }
        }
    } catch (error) {
        setIsSigningError(true);
        console.error('Error saving signature:', error);
        alert(`Failed to sign contract: ${error.response?.data?.detail || 'Unknown error'}`);
    } finally {
        setIsSigning(false);
    }
  };

  /**
   * Downloads the signed contract as a PDF
   * @async
   * @returns {Promise<void>}
   */
  const handleDownload = async () => {
    try {
      if (!contractId) {
        console.error('No contract ID available');
        return;
      }

      const response = await axios.get(
        `${API_ENDPOINTS.CONTRACTS.DOWNLOAD}/${contractId}`,
        { responseType: 'blob' }
      );
      
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'signed_contract.pdf');
      document.body.appendChild(link);
      link.click();
      link.remove();
    } catch (error) {
      console.error('Error downloading contract:', error);
    }
  };

  /**
   * Handles changes in contract version selection
   * @param {React.ChangeEvent<HTMLSelectElement>} e - The change event
   */
  const handleVersionChange = (e) => {
    const selectedValue = e.target.value;
    
    if (selectedValue === '') {
      // Show current version (latest)
      setContent(versions[0]?.content || '');
      setSelectedVersion(null);
    } else {
      // Find version by version number
      const version = versions.find(v => v.version === parseInt(selectedValue));
      if (version) {
        setContent(version.content);
        setSelectedVersion(version.version);
      }
    }
  };

  /**
   * Returns the default contract template with placeholders
   * @returns {string} The default contract template
   */
  const getDefaultTemplate = () => `
SNAPPED II, LLC PLATFORM MANAGEMENT SERVICES AGREEMENT

This agreement ("Agreement") is made and entered into as of the signature date outlined below ("Effective Date"), by and between Snapped II, LLC ("Company") on the one hand, and the ("Talent") on the other hand as outlined in the signature field below.

The parties involved, after considering and acknowledging the mutual promises and valuable considerations detailed herein, hereby agree to be bound by the terms and conditions set forth in this agreement.

1.    RECEITALS:

      1.1.    WHEREAS Snapped II, LLC has a proven track record of assisting celebrities in producing, curating, and monetizing content on Snapchat, and the Talent is seeking assistance from Snapped, LLC to grow and monetize an audience on Snapchat, leveraging a variety of content sources;

2.    ENGAGEMENT AND TERM

      2.1.    Talent hereby engages Company, during the Term (as defined in Paragraph 2.2, below), as Talent's exclusive platform manager directly relating to all activities performed directly or indirectly on Snapchat;

      2.2.    The term of this Agreement (the "Term") shall consist of an initial period commencing on the signing date of this agreement and ending upon the date that occurs twelve (12) months therefrom ("Initial Period"), plus the Option Period, if any, by which the Term may be extended pursuant to Paragraph 2.3, below;

      2.3.    This agreement will extend for an additional 24 months after the initial twelve (12) month term. If the Talent wishes to terminate this agreement after the initial term or thereafter, they must provide the Company with a 30 day written notice of termination before the end of the initial Term.

3.    COMPANY OBLIGATIONS

      3.1.    Work with the Talent and their respective teams to collect all possible content to be used on Snapchat;

      3.2.    Work with Talent and their teams to create a content plan leveraging Snapchat best practices;

      3.3.    Where applicable and when explicitly requested to do so, curate, segment, edit, and post all Snapchat content on behalf of Talent;

      3.4.    The Company will make best efforts to get Talent monetized on Snapchat.

4.    TALENT'S OBLIGATIONS & CONTENT STRATEGIES

      4.1.    Talent should create content in-line with the Company's recommendations;

              4.1.1.    Sufficient content to support posting on Snapchat seven days a week. Our average client has four minutes of content posted each day and our best performing clients are posting 10 minutes of content each day.

              4.1.1.1.    Each day should have at least three minutes of video content.

              4.1.1.2.    Talent must upload content through the Company's preferred iOS application.

      4.1.2.    If the Talent does not provide ample content in line with the Company's recommendations, the Talent's monetization on Snapchat could be significantly impacted.

5.    COMMISSIONS

      5.1.    The Company shall be entitled to a commission ("Commission") from the Talent, calculated as follows:

              5.1.1.    Fixed at thirty percent (30%) of the Talent's gross revenue, defined as the total monies collected from Snapchat for the Talent during the Term;

      5.2.    Every 30 days the Company or the Talent will withdraw money from Snapchat's wallet;

              5.2.1.    The Company will manage the Talent's Hyperwallet in tandem with the Talent. Both the Talent and the Company will have full access to the Hyperwallet account and both parties will receive their respective compensation directly from Hyperwallet.

              5.2.2.    This agreement acts as a Letter of Direction, allowing the Company to manage the Talent's Hyperwallet payments.

              5.2.3.    The Company will provide the talent with an earnings statement each month within two (2) weeks of withdrawal.

6.    TECHNOLOGY

      6.1.    Talent will use our technology throughout our partnership to manage their content on Snapchat.

      6.2.    Talent grants Company a non-exclusive, irrevocable license to use content uploaded through the Company's preferred technology platform solely for the purpose of training, developing, and improving artificial intelligence and machine learning models. This includes the right to process, analyze, and aggregate such content into datasets that may be used internally or provided to third parties for AI model training and related applications.

7.    GOVERNING LAW, OTHER, & ARBITRATION

      7.1.    Governing Law: This Agreement has been entered into in the State of Wyoming;

      7.2.    Publicity Clause: Talent grants Company the right to use Talent's name and an approved likeness for promotional purposes exclusively on the Company's website and in the Company's pitch deck. No other usage rights are granted herein. The Company agrees to use only images and the name of the Talent that have been approved in advance by the Talent.

      7.3.    Arbitration: All claims controversies, disputes, demands, counts, or causes of action arising out of, touching on, or relating to this Agreement, shall be submitted to JAMS and shall be resolved exclusively by confidential binding arbitration in Los Angeles County, California;

      7.4.    Legalities: If any provision of this Agreement is held to be invalid, illegal, or unenforceable, the validity, legality, and enforceability of the remaining provisions shall not in any way be affected or impaired thereby. The parties agree to negotiate in good faith to replace the invalid provision with a valid one that reflects the original intent of the parties;

      7.5.    Assignment: Provided that all terms and mutual understandings outlined herein remain unchanged, either party may assign this agreement at their sole discretion.

IN WITNESS WHEREOF, the parties hereto have executed this Agreement as of the date written below:











`;

  if (!isOpen) return null;

  return (
    <div className="snapped-contract-modal-overlay" onClick={onClose}>
      <div className="snapped-contract-modal" onClick={e => e.stopPropagation()}>
        {!showSignaturePad ? (
          <>
            <div className="snapped-contract-modal__header">
              <h2 className="snapped-contract-modal__title">
                Contract: {client?.First_Legal_Name} {client?.Last_Legal_Name}
              </h2>
              <div className="snapped-contract-modal__version-select">
                <select 
                  value={selectedVersion ?? ''} 
                  onChange={handleVersionChange}
                  className="snapped-contract-modal__select"
                >
                  <option value="">Current Version</option>
                  {versions.map((version, index) => (
                    <option 
                      key={version.timestamp} 
                      value={version.version || index}
                    >
                      Version {version.version || index + 1} - {new Date(version.timestamp).toLocaleString()}
                      {version.edited_by ? ` (by ${version.edited_by})` : ''}
                    </option>
                  ))}
                </select>
              </div>
              <div className="snapped-contract-modal__status">{status.toUpperCase()}</div>
            </div>

            <div className="snapped-contract-modal__editor">
              <textarea
                value={content}
                onChange={(e) => setContent(e.target.value)}
                className="snapped-contract-modal__textarea"
                rows={20}
                readOnly={isContractSigned || status === 'rep_signed'}
              />
            </div>

            <div className="snapped-contract-modal__actions">
              <button 
                onClick={handleSaveDraft} 
                className="snapped-contract-modal__btn snapped-contract-modal__btn--save"
                disabled={isSaving || isContractSigned || status === 'rep_signed'}
              >
                {isSaving ? 'Saving...' : 'Save Draft'}
              </button>
              <button 
                onClick={handleRepSign} 
                className="snapped-contract-modal__btn snapped-contract-modal__btn--sign"
                disabled={isContractSigned || status === 'rep_signed'}
              >
                Sign as Representative
              </button>
              <button 
                onClick={handleSendToClient} 
                className="snapped-contract-modal__btn snapped-contract-modal__btn--send"
                disabled={isSaving || isContractSigned}
              >
                Send to Client for Signature
              </button>
            </div>
          </>
        ) : (
          <div className="signature-container">
            <h3>Please sign below</h3>
            {showSignaturePad && (
              <div className="name-input-container">
                <label htmlFor="repName">Representative Name:</label>
                <input
                  type="text"
                  id="repName"
                  placeholder="Enter your full name"
                  onChange={(e) => setRepName(e.target.value)}
                  className="representative-name-input"
                />
              </div>
            )}
            <SignaturePad
              ref={sigPad}
              canvasProps={{
                className: "signature-pad"
              }}
            />
            <div className="signature-actions">
              <button onClick={() => sigPad.current.clear()}>Clear</button>
              <button 
                onClick={handleSign}
                disabled={isSigning || (showSignaturePad && !repName)}
              >
                {isSigning ? 'Signing...' : 'Complete Signing'}
              </button>
              {status === 'signed' && (
                <button onClick={handleDownload}>
                  Download Signed Contract
                </button>
              )}
            </div>
            {isSigningError && (
              <div className="error-message">
                Failed to save signature. Please try again.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ContractModal; 