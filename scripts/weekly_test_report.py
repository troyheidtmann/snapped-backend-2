"""
Weekly Test Report Generator

This script:
1. Runs all tests (unit, integration, e2e)
2. Collects test results and coverage data
3. Generates a detailed HTML report
4. Emails the report to specified recipients
"""

import os
import sys
import pytest
import coverage
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Any
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "admin@snapped.cc"
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = "Snapped Admin <admin@snapped.cc>"
TO_EMAILS = [
    "admin@snapped.cc",
    # Add more recipients as needed
]

class TestReportGenerator:
    def __init__(self):
        self.test_results = {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "coverage": {},
            "slow_tests": [],
            "timestamp": datetime.now().isoformat()
        }
        
    def run_tests(self):
        """Run all tests and collect results"""
        logger.info("Starting test run...")
        
        # Start coverage measurement
        cov = coverage.Coverage()
        cov.start()
        
        try:
            # Run pytest with timing
            result = pytest.main([
                "--verbose",
                "--junit-xml=test-results.xml",
                "--durations=10"  # Show 10 slowest tests
            ])
            
            # Stop coverage measurement
            cov.stop()
            cov.save()
            
            # Generate coverage report
            cov.html_report(directory="coverage_report")
            coverage_data = cov.get_data()
            total_coverage = coverage_data.get_total_covered_count() / coverage_data.get_total_lines_count() * 100
            
            # Parse test results
            self._parse_test_results("test-results.xml")
            
            # Add coverage data
            self.test_results["coverage"] = {
                "total": round(total_coverage, 2),
                "report_path": "coverage_report/index.html"
            }
            
            logger.info("Test run completed successfully")
            
        except Exception as e:
            logger.error(f"Error running tests: {str(e)}")
            self.test_results["errors"].append(str(e))
    
    def _parse_test_results(self, results_file: str):
        """Parse JUnit XML results file"""
        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(results_file)
            root = tree.getroot()
            
            # Get test counts
            self.test_results["total"] = int(root.attrib.get("tests", 0))
            self.test_results["passed"] = int(root.attrib.get("passed", 0))
            self.test_results["failed"] = int(root.attrib.get("failures", 0))
            self.test_results["skipped"] = int(root.attrib.get("skipped", 0))
            
            # Get test durations
            for testcase in root.findall(".//testcase"):
                duration = float(testcase.attrib.get("time", 0))
                if duration > 1.0:  # Tests taking more than 1 second
                    self.test_results["slow_tests"].append({
                        "name": testcase.attrib.get("name"),
                        "duration": round(duration, 2)
                    })
            
        except Exception as e:
            logger.error(f"Error parsing test results: {str(e)}")
            self.test_results["errors"].append(f"Failed to parse results: {str(e)}")
    
    def generate_html_report(self) -> str:
        """Generate HTML report from test results"""
        report = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .summary {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
                .passed {{ color: green; }}
                .failed {{ color: red; }}
                .warning {{ color: orange; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f5f5f5; }}
            </style>
        </head>
        <body>
            <h1>Weekly Test Report - {datetime.now().strftime('%Y-%m-%d')}</h1>
            
            <div class="summary">
                <h2>Test Summary</h2>
                <p>Total Tests: {self.test_results['total']}</p>
                <p class="passed">Passed: {self.test_results['passed']}</p>
                <p class="failed">Failed: {self.test_results['failed']}</p>
                <p class="warning">Skipped: {self.test_results['skipped']}</p>
                <p>Code Coverage: {self.test_results['coverage'].get('total', 0)}%</p>
            </div>
            
            {self._generate_errors_section()}
            {self._generate_slow_tests_section()}
            
            <p>Full coverage report available at: {self.test_results['coverage'].get('report_path')}</p>
        </body>
        </html>
        """
        return report
    
    def _generate_errors_section(self) -> str:
        """Generate HTML section for test errors"""
        if not self.test_results["errors"]:
            return ""
            
        errors_html = """
        <h2 class="failed">Errors</h2>
        <ul>
        """
        for error in self.test_results["errors"]:
            errors_html += f"<li>{error}</li>"
        errors_html += "</ul>"
        return errors_html
    
    def _generate_slow_tests_section(self) -> str:
        """Generate HTML section for slow tests"""
        if not self.test_results["slow_tests"]:
            return ""
            
        slow_tests_html = """
        <h2 class="warning">Slow Tests</h2>
        <table>
            <tr>
                <th>Test Name</th>
                <th>Duration (seconds)</th>
            </tr>
        """
        for test in self.test_results["slow_tests"]:
            slow_tests_html += f"""
            <tr>
                <td>{test['name']}</td>
                <td>{test['duration']}</td>
            </tr>
            """
        slow_tests_html += "</table>"
        return slow_tests_html
    
    def send_email_report(self, html_report: str):
        """Send HTML report via email"""
        try:
            # Create message
            msg = MIMEMultipart()
            msg["Subject"] = f"Weekly Test Report - {datetime.now().strftime('%Y-%m-%d')}"
            msg["From"] = FROM_EMAIL
            msg["To"] = ", ".join(TO_EMAILS)
            
            # Attach HTML report
            msg.attach(MIMEText(html_report, "html"))
            
            # Attach coverage report if exists
            coverage_path = Path("coverage_report/index.html")
            if coverage_path.exists():
                with open(coverage_path, "rb") as f:
                    attachment = MIMEApplication(f.read(), _subtype="html")
                    attachment.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename="coverage_report.html"
                    )
                    msg.attach(attachment)
            
            # Send email
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
                
            logger.info("Test report email sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending email report: {str(e)}")
            raise

def main():
    """Main execution function"""
    try:
        # Create report generator
        generator = TestReportGenerator()
        
        # Run tests and generate report
        generator.run_tests()
        html_report = generator.generate_html_report()
        
        # Send email report
        generator.send_email_report(html_report)
        
        # Save results to file
        with open("test_results.json", "w") as f:
            json.dump(generator.test_results, f, indent=2)
            
        logger.info("Weekly test report generated and sent successfully")
        
    except Exception as e:
        logger.error(f"Error in weekly test report generation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 