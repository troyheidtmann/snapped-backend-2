/**
 * @fileoverview Timezone data organized by regions for use in signup and settings forms.
 * Provides a comprehensive list of timezones with their GMT offsets and locations.
 * 
 * @typedef {Object.<string, string[]>} TimezoneData
 * @property {string[]} "United States" - US timezone options
 * @property {string[]} "Australia & New Zealand" - AU/NZ timezone options
 * @property {string[]} "Middle East & North Africa" - MENA timezone options
 * @property {string[]} "Americas" - Americas timezone options
 * @property {string[]} "Europe" - European timezone options
 * @property {string[]} "Asia Pacific" - APAC timezone options
 * @property {string[]} "Other Regions" - Additional timezone options
 */

/** @type {TimezoneData} */
export const TIMEZONES = {
  "United States": [
    "(GMT -10:00) Pacific/Honolulu    - Hawaii",
    "(GMT -08:00) America/Los_Angeles - Pacific Time",
    "(GMT -07:00) America/Phoenix     - Mountain Time (AZ)",
    "(GMT -07:00) America/Denver      - Mountain Time",
    "(GMT -06:00) America/Chicago     - Central Time",
    "(GMT -05:00) America/New_York    - Eastern Time"
  ],
  
  "Australia & New Zealand": [
    "(GMT +08:00) Australia/Perth     - Western Australia",
    "(GMT +09:30) Australia/Darwin    - Northern Territory",
    "(GMT +09:30) Australia/Adelaide  - South Australia",
    "(GMT +10:00) Australia/Brisbane  - Queensland",
    "(GMT +10:00) Australia/Sydney    - NSW/Victoria",
    "(GMT +10:30) Australia/Lord_Howe - Lord Howe Island",
    "(GMT +12:00) Pacific/Auckland    - New Zealand"
  ],

  "Middle East & North Africa": [
    "(GMT +02:00) Africa/Cairo        - Egypt",
    "(GMT +03:00) Asia/Baghdad        - Iraq",
    "(GMT +03:00) Asia/Kuwait         - Kuwait",
    "(GMT +03:00) Asia/Riyadh         - Saudi Arabia",
    "(GMT +03:00) Asia/Qatar          - Qatar",
    "(GMT +03:30) Asia/Tehran         - Iran",
    "(GMT +04:00) Asia/Dubai          - UAE",
    "(GMT +04:00) Asia/Muscat         - Oman"
  ],

  "Americas": [
    "(GMT -09:00) America/Anchorage   - Alaska",
    "(GMT -04:00) America/Halifax     - Atlantic Time",
    "(GMT -03:00) America/Sao_Paulo   - Brazil",
    "(GMT -03:00) America/Argentina/Buenos_Aires",
    "(GMT -04:00) America/Puerto_Rico - Caribbean",
    "(GMT -06:00) America/Mexico_City - Mexico",
    "(GMT -05:00) America/Toronto     - Eastern Canada"
  ],

  "Europe": [
    "(GMT +00:00) Europe/London       - UK",
    "(GMT +01:00) Europe/Paris        - France",
    "(GMT +01:00) Europe/Berlin       - Germany",
    "(GMT +02:00) Europe/Helsinki     - Finland",
    "(GMT +02:00) Europe/Kiev         - Ukraine",
    "(GMT +03:00) Europe/Moscow       - Russia"
  ],

  "Asia Pacific": [
    "(GMT +07:00) Asia/Bangkok        - Thailand",
    "(GMT +07:00) Asia/Jakarta        - Indonesia",
    "(GMT +08:00) Asia/Shanghai       - China",
    "(GMT +08:00) Asia/Singapore      - Singapore",
    "(GMT +09:00) Asia/Tokyo          - Japan",
    "(GMT +09:00) Asia/Seoul          - Korea",
    "(GMT +05:30) Asia/Kolkata        - India"
  ],

  "Other Regions": [
    "(GMT -10:00) America/Adak",
    "(GMT -09:00) Pacific/Gambier",
    "(GMT -09:30) Pacific/Marquesas",
    "(GMT -09:00) Pacific/Pago_Pago",
    "(GMT -08:00) Pacific/Pitcairn",
    "(GMT -03:30) America/St_Johns",
    "(GMT -02:00) America/Noronha",
    "(GMT -01:00) Atlantic/Azores",
    "(GMT -01:00) Atlantic/Cape_Verde",
    "(GMT +00:00) UTC",
    "(GMT +04:00) Asia/Baku",
    "(GMT +04:30) Asia/Kabul",
    "(GMT +05:00) Asia/Karachi",
    "(GMT +05:45) Asia/Kathmandu",
    "(GMT +06:00) Asia/Dhaka",
    "(GMT +06:30) Asia/Rangoon",
    "(GMT +08:30) Asia/Pyongyang",
    "(GMT +11:00) Asia/Magadan",
    "(GMT +11:00) Pacific/Norfolk",
    "(GMT +11:30) Pacific/Apia",
    "(GMT +12:00) Pacific/Fiji",
    "(GMT +12:45) Pacific/Chatham",
    "(GMT +13:00) Pacific/Tongatapu",
    "(GMT +14:00) Pacific/Kiritimati"
  ]
}; 