import subprocess
import sys
import os
import time
from datetime import datetime

def run_tests(with_coverage=True):
    print("\n" + "="*80)
    print(f"RUNNING TESTS AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")

    try:
        if with_coverage:
            try:
                # Try running tests with coverage report
                print("Running tests with coverage report...\n")
                result = subprocess.run(
                    ["pytest", "-v", "--cov=.", "--cov-report=term", "--cov-report=html"],
                    capture_output=True,
                    text=True
                )

                # If coverage fails, fall back to regular tests
                if result.returncode != 0 and "unrecognized arguments: --cov" in result.stderr:
                    print("Coverage reporting not available. Falling back to standard tests...\n")
                    return run_tests(with_coverage=False)
            except Exception as e:
                print(f"Error running tests with coverage: {str(e)}")
                print("Falling back to standard tests...\n")
                return run_tests(with_coverage=False)
        else:
            # Run tests without coverage report
            print("Running tests without coverage report...\n")
            result = subprocess.run(
                ["pytest", "-v"],
                capture_output=True,
                text=True
            )

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            print("\n✅ All tests passed successfully!")
            if with_coverage:
                print("Coverage report generated in htmlcov/ directory")
                print("Open htmlcov/index.html in a browser to view the detailed report")
            return True
        else:
            print("\n❌ Some tests failed!")
            return False
    except Exception as e:
        print(f"\n❌ Error running tests: {str(e)}")
        return False

def check_docker_installed():
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except Exception:
        return False

def build_docker():
    print("\n" + "="*80)
    print("BUILDING DOCKER IMAGE")
    print("="*80 + "\n")

    if not check_docker_installed():
        print("❌ Docker is not installed or not running!")
        return False

    try:
        result = subprocess.run(
            ["docker", "build", "-t", "fastapi_app", "."],
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            print("\n✅ Docker build successful!")
            return True
        else:
            print("\n❌ Docker build failed!")
            return False
    except Exception as e:
        print(f"\n❌ Error building Docker image: {str(e)}")
        return False

if __name__ == "__main__":
    start_time = time.time()

    tests_passed = run_tests()

    if tests_passed:
        print("\n" + "="*80)
        print("TESTS PASSED - PROCEEDING WITH DOCKER BUILD")
        print("="*80 + "\n")

        build_success = build_docker()

        if build_success:
            print("\n" + "="*80)
            print("BUILD SUCCESSFUL")
            print("="*80)
            print("\nYou can now run the container with:")
            print("docker run -p 8000:8000 fastapi_app")
            print("\nAccess the API at:")
            print("- Upload CSV: curl -X 'POST' 'http://127.0.0.1:8000/upload/' -F 'file=@file_name.csv'")
            print("- Fetch records: curl -X 'GET' 'http://127.0.0.1:8000/records/'")

            elapsed_time = time.time() - start_time
            print(f"\nTotal execution time: {elapsed_time:.2f} seconds")
            sys.exit(0)
        else:
            print("\n❌ Docker build failed! Please check the error messages above.")
            sys.exit(1)
    else:
        print("\n" + "="*80)
        print("TESTS FAILED - DOCKER BUILD NOT TRIGGERED")
        print("="*80)
        print("\n❌ Fix the failing tests before proceeding with the Docker build.")
        sys.exit(1)
