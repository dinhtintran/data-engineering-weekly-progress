#!/usr/bin/env python3
"""
Script to run all unit tests
"""
import unittest
import sys
import os
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def discover_and_run_tests():
    """Discover and run all tests in the tests directory"""
    # Discover tests in the tests directory
    loader = unittest.TestLoader()
    suite = loader.discover('tests', pattern='test_*.py')
    
    # Run tests with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Generate test report
    generate_test_report(result)
    
    # Return exit code based on test results
    return 0 if result.wasSuccessful() else 1

def generate_test_report(result):
    """Generate a test report file"""
    report_dir = os.path.join(os.path.dirname(__file__), 'test_reports')
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(report_dir, f'test_report_{timestamp}.txt')
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("TEST REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"Tests run: {result.testsRun}\n")
        f.write(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}\n")
        f.write(f"Failures: {len(result.failures)}\n")
        f.write(f"Errors: {len(result.errors)}\n")
        f.write(f"Success rate: {(result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100:.1f}%\n\n")
        
        if result.failures:
            f.write("=" * 80 + "\n")
            f.write("FAILURES\n")
            f.write("=" * 80 + "\n")
            for test, traceback in result.failures:
                f.write(f"\n{test}\n")
                f.write("-" * 80 + "\n")
                f.write(traceback + "\n")
        
        if result.errors:
            f.write("=" * 80 + "\n")
            f.write("ERRORS\n")
            f.write("=" * 80 + "\n")
            for test, traceback in result.errors:
                f.write(f"\n{test}\n")
                f.write("-" * 80 + "\n")
                f.write(traceback + "\n")
        
        if result.wasSuccessful():
            f.write("\n" + "=" * 80 + "\n")
            f.write("ALL TESTS PASSED! ✓\n")
            f.write("=" * 80 + "\n")
    
    print(f"\n📊 Test report saved to: {report_file}")
    return report_file

if __name__ == '__main__':
    exit_code = discover_and_run_tests()
    sys.exit(exit_code)

