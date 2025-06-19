#!/bin/bash

# Weekly Test Report Cron Script
# Recommended to run weekly on Sunday at 12:00 AM
# Cron expression: 0 0 * * 0

# Set environment variables
export PYTHONPATH=/home/ubuntu/snappedii
export SMTP_PASSWORD="your_email_password"  # Set this in cron environment
export MONGODB_TEST_URL="your_test_mongodb_url"  # Set this in cron environment

# Navigate to project directory
cd /home/ubuntu/snappedii

# Activate virtual environment if using one
# source venv/bin/activate

# Create logs directory if it doesn't exist
mkdir -p logs/test_reports

# Get current date for log file
DATE=$(date +%Y-%m-%d)
LOG_FILE="logs/test_reports/weekly_test_report_${DATE}.log"

# Run the test report script
echo "Starting weekly test report at $(date)" > "$LOG_FILE"
python3 scripts/weekly_test_report.py >> "$LOG_FILE" 2>&1

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo "Weekly test report completed successfully at $(date)" >> "$LOG_FILE"
else
    echo "Weekly test report failed at $(date)" >> "$LOG_FILE"
    # Could add notification here for failed runs
fi

# Cleanup old reports (keep last 3 months)
find logs/test_reports -type f -mtime +90 -delete

# Cleanup old coverage reports (keep last week)
find coverage_report -type f -mtime +7 -delete 