/**
 * @fileoverview A real-time chat interface component that enables communication with an AI assistant.
 * The component supports user selection, message history, and markdown rendering for AI responses.
 */

import React, { useState, useRef, useEffect } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import './AIChat.css';
import { API_ENDPOINTS } from '../../config/api';
import Markdown from 'markdown-to-jsx';
import axios from 'axios';

/**
 * @typedef {Object} Message
 * @property {'user' | 'assistant'} role - The role of the message sender
 * @property {string} content - The content of the message
 * @property {Date} timestamp - When the message was sent
 */

/**
 * @typedef {Object} User
 * @property {string} client_id - Unique identifier for the client
 * @property {string} [name] - Optional display name for the client
 */

/**
 * ChatInterface component provides a real-time messaging interface with AI.
 * Features include:
 * - User selection dropdown
 * - Real-time message history
 * - Markdown rendering for AI responses
 * - Loading states and error handling
 * - Auto-scrolling to latest messages
 * 
 * @returns {React.ReactElement} The rendered chat interface
 */
const ChatInterface = () => {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [users, setUsers] = useState([]);
    const [selectedUser, setSelectedUser] = useState('');
    const [isLoadingUsers, setIsLoadingUsers] = useState(false);
    const [isLoadingHistory, setIsLoadingHistory] = useState(false);
    const [permissionError, setPermissionError] = useState(null);
    const messagesEndRef = useRef(null);
    const chatMessagesRef = useRef(null);
    const { getAccessToken } = useAuth();

    // Fetch users on component mount
    useEffect(() => {
        const fetchUsers = async () => {
            try {
                setIsLoadingUsers(true);
                setPermissionError(null);
                
                const response = await axios.get(API_ENDPOINTS.DESKTOP_UPLOAD.USERS);
                
                if (response.data.status === 'success') {
                    setUsers(response.data.users);
                    if (response.data.users.length === 0) {
                        setPermissionError("You don't have access to any users. Please contact an administrator.");
                    }
                }
            } catch (error) {
                console.error('Error fetching users:', error);
                if (error.response?.status === 403) {
                    setPermissionError("You don't have permission to access this feature.");
                }
            } finally {
                setIsLoadingUsers(false);
            }
        };
        fetchUsers();
    }, []);

    // Function to scroll to bottom of messages
    const scrollToBottom = () => {
        if (messagesEndRef.current) {
            messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
        }
    };

    // Fetch chat history when user is selected
    useEffect(() => {
        const fetchChatHistory = async () => {
            if (!selectedUser) return;
            
            try {
                setIsLoadingHistory(true);
                const token = await getAccessToken();
                const response = await fetch(API_ENDPOINTS.CHAT.HISTORY(selectedUser), {
                    headers: {
                        'Authorization': `Bearer ${token}`
                    }
                });
                
                const data = await response.json();
                if (data.status === 'success' && Array.isArray(data.messages)) {
                    // Convert timestamps to Date objects
                    const formattedMessages = data.messages.map(msg => ({
                        ...msg,
                        timestamp: new Date(msg.timestamp)
                    }));
                    setMessages(formattedMessages);
                    // Scroll to bottom after messages are loaded
                    setTimeout(scrollToBottom, 100); // Small delay to ensure messages are rendered
                }
            } catch (error) {
                console.error('Error fetching chat history:', error);
                setMessages([]);
            } finally {
                setIsLoadingHistory(false);
            }
        };

        fetchChatHistory();
    }, [selectedUser, getAccessToken]);

    // Scroll to bottom when new messages are added
    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    useEffect(() => {
        // Remove recent-message class from previous message
        const prevMessage = document.querySelector('.recent-message');
        if (prevMessage) {
            prevMessage.classList.remove('recent-message');
            // Reset any absolute positioning
            prevMessage.style.position = 'relative';
            prevMessage.style.top = 'auto';
            prevMessage.style.left = 'auto';
            prevMessage.style.transform = 'none';
        }

        // Add recent-message class to new message
        if (messagesEndRef.current) {
            const lastMessage = messagesEndRef.current.previousElementSibling;
            if (lastMessage && lastMessage.classList.contains('message')) {
                lastMessage.classList.add('recent-message');
            }
        }
    }, [messages]);

    const handleSend = async () => {
        if (!input.trim() || !selectedUser) return;

        const userMessage = {
            role: 'user',
            content: input,
            timestamp: new Date()
        };

        setMessages(prev => [...prev, userMessage]);
        setInput('');
        setLoading(true);

        try {
            const token = await getAccessToken();
            const response = await fetch(API_ENDPOINTS.CHAT.SEND, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${token}`
                },
                body: JSON.stringify({
                    messages: [...messages, userMessage],
                    client_id: selectedUser
                })
            });

            const data = await response.json();
            
            setMessages(prev => [...prev, {
                role: 'assistant',
                content: data.message,
                timestamp: new Date(data.timestamp)
            }]);
        } catch (error) {
            console.error('Error:', error);
            setMessages(prev => [...prev, {
                role: 'assistant',
                content: 'Sorry, I encountered an error. Please try again.',
                timestamp: new Date()
            }]);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="chat-page-wrapper">
            <div className="chat-container">
                <div className="chat-input">
                    <div className="chat-input-wrapper">
                        {/* Client selector dropdown */}
                        <select 
                            value={selectedUser} 
                            onChange={(e) => setSelectedUser(e.target.value)}
                            className="client-select"
                            disabled={isLoadingUsers}
                            style={{ marginBottom: '16px' }}
                        >
                            <option value="">Select Client</option>
                            {users.map(user => (
                                <option key={user.client_id} value={user.client_id}>
                                    {user.name || user.client_id}
                                </option>
                            ))}
                        </select>

                        {permissionError && (
                            <div className="permission-error-message">
                                {permissionError}
                            </div>
                        )}

                        <textarea
                            value={input}
                            onChange={(e) => setInput(e.target.value)}
                            onKeyPress={(e) => e.key === 'Enter' && !e.shiftKey && handleSend()}
                            placeholder={selectedUser ? "Ask anything about this client..." : "Please select a client first"}
                            disabled={!selectedUser || isLoadingHistory}
                        />
                    </div>
                </div>
                <div className="chat-messages">
                    <div className="messages-viewport">
                        <div className="messages-content">
                            {isLoadingHistory ? (
                                <div className="message assistant">
                                    <div className="typing-indicator">
                                        <span></span>
                                        <span></span>
                                        <span></span>
                                    </div>
                                </div>
                            ) : (
                                messages.map((message, index) => (
                                    <div 
                                        key={index} 
                                        className={`message ${message.role === 'user' ? 'user' : 'assistant'}`}
                                    >
                                        <div className="message-content">
                                            {message.role === 'user' ? (
                                                message.content
                                            ) : (
                                                <Markdown options={{
                                                    forceBlock: true,
                                                    overrides: {
                                                        h1: { props: { className: 'message-heading-1' } },
                                                        h2: { props: { className: 'message-heading-2' } },
                                                        h3: { props: { className: 'message-heading-3' } },
                                                        p: { props: { className: 'message-paragraph' } },
                                                        ul: { props: { className: 'message-list' } },
                                                        li: { props: { className: 'message-list-item' } },
                                                        code: { props: { className: 'message-code' } },
                                                    },
                                                }}>
                                                    {message.content}
                                                </Markdown>
                                            )}
                                        </div>
                                    </div>
                                ))
                            )}
                            {loading && (
                                <div className="message assistant recent-message">
                                    <div className="typing-indicator">
                                        <span></span>
                                        <span></span>
                                        <span></span>
                                    </div>
                                </div>
                            )}
                            <div ref={messagesEndRef} style={{ height: '1px' }} />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ChatInterface;
