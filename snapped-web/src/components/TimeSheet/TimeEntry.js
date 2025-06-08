/**
 * @fileoverview TimeEntry model for tracking time entries and invoicing.
 * Defines the schema for time tracking records in the database.
 */

import mongoose from 'mongoose';

/**
 * @typedef {Object} TimeEntry
 * @property {string} user_id - ID of the user who created the entry
 * @property {Date} date - Date of the time entry
 * @property {string} client_id - Associated client ID
 * @property {number} hours - Number of hours worked
 * @property {number} minutes - Number of minutes worked
 * @property {string} type - Type of work performed
 * @property {string} item - Specific work item identifier
 * @property {string} description - Detailed description of work performed
 * @property {Date} created_at - Timestamp when entry was created
 * @property {string} invoice_id - Associated invoice ID if any
 * @property {string} invoice_status - Status of the invoice (draft, submitted, paid)
 */

/**
 * Mongoose schema for time entries.
 * Defines the structure and validation rules for time tracking records.
 * 
 * @type {mongoose.Schema}
 */
const timeEntrySchema = new mongoose.Schema({
  user_id: {
    type: String,
    required: true,
    index: true
  },
  date: {
    type: Date,
    required: true
  },
  client_id: {
    type: String,
    required: true,
    ref: 'Client'
  },
  hours: {
    type: Number,
    required: true
  },
  minutes: {
    type: Number,
    required: true
  },
  type: {
    type: String,
    required: true
  },
  item: {
    type: String,
    required: true
  },
  description: {
    type: String,
    required: true
  },
  created_at: {
    type: Date,
    default: Date.now
  },
  invoice_id: {
    type: String,
    default: null
  },
  invoice_status: {
    type: String,
    enum: ['draft', 'submitted', 'paid'],
    default: 'draft'
  }
});

/**
 * TimeEntry model for managing time tracking records.
 * Collection name is set to 'time_track'.
 * 
 * @type {mongoose.Model<TimeEntry>}
 */
export const TimeEntry = mongoose.model('TimeEntry', timeEntrySchema, 'time_track'); 