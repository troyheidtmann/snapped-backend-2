/**
 * @fileoverview Privacy Policy component that displays the application's privacy policy,
 * including data collection practices, permissions, storage, and user rights.
 */

import React from 'react';
import './Privacy.css';

/**
 * Privacy Policy component that renders a comprehensive privacy policy document.
 * Sections include:
 * - Data collection and usage
 * - App permissions
 * - Data storage and processing
 * - Third-party services
 * - Data tracking
 * - Data deletion and account removal
 * - iOS data access
 * - Data protection and security
 * - User rights
 * - Children's privacy
 * - California privacy rights
 * - Contact information
 * 
 * @returns {React.ReactElement} The rendered privacy policy document
 */
const Privacy = () => {
  return (
    <div className="privacy-container">
      <div className="privacy-content">
        <h1>Privacy Policy</h1>
        <div className="privacy-section app-identifier">
          <p>
            This privacy policy applies to the iOS application and all related services.
          </p>
        </div>
        <div className="privacy-section">
          <h2>Data Collection and Usage</h2>
          <p>
            Our iOS app collects and processes the following types of information:
          </p>
          <ul>
            <li>Media files (photos and videos) that you explicitly choose to upload</li>
            <li>Device information required for app functionality</li>
            <li>Authentication credentials for secure access</li>
            <li>Usage analytics to improve app performance</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>App Permissions</h2>
          <p>
            To provide our services, we may request access to:
          </p>
          <ul>
            <li>Photo Library - For uploading content</li>
            <li>Camera - For capturing new content</li>
            <li>Network Access - For content delivery</li>
            <li>Push Notifications - For important updates</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>Data Storage and Processing</h2>
          <p>
            All uploaded content is:
          </p>
          <ul>
            <li>Securely transmitted using industry-standard encryption</li>
            <li>Stored in our secure CDN infrastructure</li>
            <li>Processed according to your explicit permissions</li>
            <li>Managed through your account settings</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>Third-Party Services</h2>
          <p>
            Our app integrates with:
          </p>
          <ul>
            <li>AWS Amplify for authentication</li>
            <li>Content Delivery Networks for media storage</li>
            <li>Analytics tools for app performance monitoring</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>Data Tracking</h2>
          <p>
            In accordance with App Store guidelines and Apple's App Tracking Transparency framework:
          </p>
          <ul>
            <li>We may request permission to track your activity across other companies' apps and websites</li>
            <li>You can opt-out of tracking by selecting "Ask App Not to Track" when prompted</li>
            <li>Your choice will be respected across all features of our app</li>
            <li>Declining tracking will not limit the functionality of the app</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>Data Deletion & Account Removal</h2>
          <p>
            As required by the App Store, we provide the following methods to delete your data:
          </p>
          <ul>
            <li>In-App Deletion: Access the Settings menu and select "Delete Account"</li>
            <li>Email Request: Contact privacy@snapped.cc with your deletion request</li>
            <li>Web Portal: Visit https://snapped.cc/account/deletion</li>
          </ul>
          <p>
            Account deletion will remove all personal data, uploaded content, and usage history 
            within 7 days of the request. Some information may be retained for legal compliance.
          </p>
        </div>

        <div className="privacy-section">
          <h2>iOS Data Access</h2>
          <p>
            Our app may access the following iOS data types:
          </p>
          <ul>
            <li>Photos - To allow content uploads (Required)</li>
            <li>Camera - For capturing new content (Optional)</li>
            <li>Location - For content metadata (Optional, can be disabled)</li>
            <li>Device ID - For account security</li>
            <li>Usage Data - For app performance</li>
          </ul>
          <p>
            All permissions can be managed in iOS Settings → Snapped Upload Manager
          </p>
        </div>

        <div className="privacy-section">
          <h2>Data Protection & Security</h2>
          <p>
            We implement Apple's recommended security practices including:
          </p>
          <ul>
            <li>End-to-end encryption for data transmission</li>
            <li>Secure enclave for credential storage</li>
            <li>App Transport Security (ATS) requirements</li>
            <li>Regular security audits and updates</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>User Rights</h2>
          <p>
            You have the right to:
          </p>
          <ul>
            <li>Access your uploaded content</li>
            <li>Delete your content and account</li>
            <li>Opt-out of analytics collection</li>
            <li>Manage app permissions through iOS settings</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>Children's Privacy</h2>
          <p>
            Our services are not intended for users under the age of 13. We do not knowingly 
            collect information from children under 13 years of age. If we discover that a 
            child under 13 has provided us with personal information, we will immediately:
          </p>
          <ul>
            <li>Delete all information associated with the account</li>
            <li>Take reasonable measures to ensure no further collection occurs</li>
            <li>Notify the parent or guardian if possible</li>
          </ul>
        </div>

        <div className="privacy-section">
          <h2>California Privacy Rights</h2>
          <p>
            California residents have additional rights regarding their personal information 
            under the CCPA. Visit our CCPA page for more details.
          </p>
        </div>

        <div className="privacy-section">
          <h2>Contact & Support</h2>
          <p>
            For privacy-related inquiries, data requests, or technical support:
          </p>
          <ul>
            <li>Email: privacy@snapped.cc</li>
            <li>Support: https://snapped.cc/support</li>
            <li>Phone: [Your Support Phone]</li>
            <li>Address: [Your Physical Address]</li>
          </ul>
        </div>

        <div className="privacy-footer">
          <p>Last updated: {new Date().toLocaleDateString()}</p>
          <p>Effective Date: {new Date().toLocaleDateString()}</p>
        </div>
      </div>
    </div>
  );
};

export default Privacy; 