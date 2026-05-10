"""
Database Connection Tester
Tests all user connections to the PostgreSQL database server.
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


class DatabaseConnectionTester:
    """Test database connections for all configured users."""
    
    def __init__(self, env_path: str = ".env"):
        """Initialize the tester and load environment variables."""
        self.env_path = env_path
        self.results = {}
        self.db_config = {}
        self.users = {}
        
        self._load_env()
    
    def _load_env(self) -> None:
        """Load environment variables from .env file."""
        if not os.path.exists(self.env_path):
            raise FileNotFoundError(f".env file not found at {self.env_path}")
        
        load_dotenv(self.env_path)
        
        # Load database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
        }
        
        # Load users
        self.users = {
            'superadmin': {
                'username': os.getenv('SUPERADMIN_USERNAME'),
                'password': os.getenv('SUPERADMIN_PASSWORD'),
            },
            'backend_user': {
                'username': os.getenv('BACKEND_USER_USERNAME'),
                'password': os.getenv('BACKEND_USER_PASSWORD'),
            },
            'airflow_user': {
                'username': os.getenv('AIRFLOW_USER_USERNAME'),
                'password': os.getenv('AIRFLOW_USER_PASSWORD'),
            },
            'etl_user': {
                'username': os.getenv('ETL_USER_USERNAME'),
                'password': os.getenv('ETL_USER_PASSWORD'),
            },
            'ml_user': {
                'username': os.getenv('ML_USER_USERNAME'),
                'password': os.getenv('ML_USER_PASSWORD'),
            },
            'readonly_user': {
                'username': os.getenv('READONLY_USER_USERNAME'),
                'password': os.getenv('READONLY_USER_PASSWORD'),
            },
            'audit_writer': {
                'username': os.getenv('AUDIT_WRITER_USERNAME'),
                'password': os.getenv('AUDIT_WRITER_PASSWORD'),
            },
        }
        
        # Validate configuration
        if not all([self.db_config['host'], self.db_config['database']]):
            raise ValueError("Missing required database configuration (host, database)")
    
    def test_connection(self, user_role: str, username: str, password: str) -> dict:
        """
        Test a single user's connection.
        
        Args:
            user_role: Role/name of the user
            username: Database username
            password: Database password
        
        Returns:
            Dictionary with test results
        """
        result = {
            'role': user_role,
            'username': username,
            'status': 'FAILED',
            'duration_ms': 0,
            'error': None,
            'message': None,
        }
        
        start_time = time.time()
        conn = None
        
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=username,
                password=password,
                connect_timeout=10
            )
            
            # Try a simple query to verify the connection works
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            cursor.close()
            
            result['status'] = 'SUCCESS'
            result['message'] = f"Connected to {version[0][:50]}..."
            
        except psycopg2.OperationalError as e:
            result['error'] = f"Operational Error: {str(e)}"
            result['status'] = 'FAILED'
        except psycopg2.Error as e:
            result['error'] = f"Database Error: {str(e)}"
            result['status'] = 'FAILED'
        except Exception as e:
            result['error'] = f"Unexpected Error: {str(e)}"
            result['status'] = 'FAILED'
        finally:
            if conn:
                conn.close()
        
        result['duration_ms'] = round((time.time() - start_time) * 1000, 2)
        return result
    
    def run_all_tests(self) -> None:
        """Run connection tests for all users."""
        print("\n" + "=" * 80)
        print(f"Database Connection Test Suite")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print(f"\nDatabase Server: {self.db_config['host']}:{self.db_config['port']}")
        print(f"Database Name: {self.db_config['database']}")
        print(f"\nTesting {len(self.users)} user(s)...\n")
        
        for role, creds in self.users.items():
            username = creds['username']
            password = creds['password']
            
            if not username or not password:
                print(f"⚠️  {role.upper():<20} - SKIPPED (credentials not configured)")
                self.results[role] = {
                    'status': 'SKIPPED',
                    'error': 'Credentials not configured',
                }
                continue
            
            result = self.test_connection(role, username, password)
            self.results[role] = result
            
            status_symbol = "✓" if result['status'] == 'SUCCESS' else "✗"
            print(f"{status_symbol} {role.upper():<20} - {result['status']:<10} ({result['duration_ms']}ms)")
            
            if result['message']:
                print(f"   └─ {result['message']}")
            if result['error']:
                print(f"   └─ ERROR: {result['error']}")
    
    def print_summary(self) -> None:
        """Print a summary of the test results."""
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        
        successful = sum(1 for r in self.results.values() if r['status'] == 'SUCCESS')
        failed = sum(1 for r in self.results.values() if r['status'] == 'FAILED')
        skipped = sum(1 for r in self.results.values() if r['status'] == 'SKIPPED')
        
        print(f"\nResults:")
        print(f"  ✓ Successful: {successful}")
        print(f"  ✗ Failed:     {failed}")
        print(f"  ⚠ Skipped:    {skipped}")
        print(f"  Total:        {len(self.results)}")
        
        if failed > 0:
            print(f"\nFailed connections:")
            for role, result in self.results.items():
                if result['status'] == 'FAILED':
                    print(f"  - {role}: {result['error']}")
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80 + "\n")
        
        return failed == 0
    
    def export_results(self, output_file: str = "connection_test_results.json") -> None:
        """Export test results to a JSON file."""
        import json
        
        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'database_host': self.db_config['host'],
                'database_name': self.db_config['database'],
                'results': self.results,
            }, f, indent=2)
        
        print(f"Results exported to: {output_file}")


def main():
    """Main entry point."""
    try:
        # Change to the script's directory
        script_dir = Path(__file__).parent
        os.chdir(script_dir)
        
        tester = DatabaseConnectionTester(env_path=".env")
        tester.run_all_tests()
        success = tester.print_summary()
        tester.export_results()
        
        return 0 if success else 1
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        print(f"   Make sure the .env file exists in the database directory", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"❌ Configuration Error: {e}", file=sys.stderr)
        print(f"   Make sure all required environment variables are set in .env", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"❌ Unexpected Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
