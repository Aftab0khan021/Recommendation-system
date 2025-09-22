import requests
import sys
import json
from datetime import datetime
import time

class RecommendationSystemTester:
    def __init__(self, base_url="https://rec-system-hub.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_user = "demo_user_1"

    def run_test(self, name, method, endpoint, expected_status, data=None, params=None):
        """Run a single API test"""
        url = f"{self.api_url}/{endpoint}"
        headers = {'Content-Type': 'application/json'}

        self.tests_run += 1
        print(f"\n🔍 Testing {name}...")
        print(f"   URL: {url}")
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=30)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=headers, timeout=30)

            success = response.status_code == expected_status
            if success:
                self.tests_passed += 1
                print(f"✅ Passed - Status: {response.status_code}")
                try:
                    response_data = response.json()
                    print(f"   Response keys: {list(response_data.keys()) if isinstance(response_data, dict) else 'Non-dict response'}")
                except:
                    print(f"   Response: {response.text[:100]}...")
            else:
                print(f"❌ Failed - Expected {expected_status}, got {response.status_code}")
                print(f"   Response: {response.text[:200]}...")

            return success, response.json() if response.text and response.status_code < 500 else {}

        except requests.exceptions.Timeout:
            print(f"❌ Failed - Request timeout (30s)")
            return False, {}
        except Exception as e:
            print(f"❌ Failed - Error: {str(e)}")
            return False, {}

    def test_health_check(self):
        """Test health check endpoint"""
        success, response = self.run_test(
            "Health Check",
            "GET",
            "health",
            200
        )
        if success:
            services = response.get('services', {})
            print(f"   Services status: {services}")
        return success

    def test_recommendations(self):
        """Test recommendations endpoint"""
        success, response = self.run_test(
            "Get Recommendations",
            "GET",
            "recommend",
            200,
            params={
                "user_id": self.test_user,
                "n": 10
            }
        )
        if success:
            recommendations = response.get('recommendations', [])
            algorithm = response.get('algorithm', 'unknown')
            print(f"   Got {len(recommendations)} recommendations using {algorithm}")
        return success, response

    def test_simple_search(self):
        """Test simple search functionality"""
        success, response = self.run_test(
            "Simple Search",
            "GET",
            "search",
            200,
            params={
                "q": "programming",
                "search_type": "simple",
                "user_id": self.test_user,
                "limit": 10
            }
        )
        if success:
            results = response.get('results', [])
            print(f"   Found {len(results)} search results")
        return success, response

    def test_ai_search(self):
        """Test AI search functionality"""
        success, response = self.run_test(
            "AI Search",
            "GET",
            "search",
            200,
            params={
                "q": "Show me educational programming courses",
                "search_type": "ai",
                "user_id": self.test_user,
                "limit": 10
            }
        )
        if success:
            results = response.get('results', [])
            print(f"   AI search found {len(results)} results")
        return success, response

    def test_log_event(self):
        """Test event logging"""
        event_data = {
            "user_id": self.test_user,
            "item_id": "test_item_1",
            "type": "view",
            "ts": datetime.utcnow().isoformat(),
            "dwell_seconds": 30,
            "context": {"source": "test"}
        }
        
        success, response = self.run_test(
            "Log Event",
            "POST",
            "event",
            200,
            data=event_data
        )
        if success:
            interaction_id = response.get('interaction_id')
            print(f"   Logged interaction: {interaction_id}")
        return success

    def test_ab_testing(self):
        """Test A/B testing endpoint"""
        success, response = self.run_test(
            "A/B Test Assignment",
            "GET",
            "ab/arm",
            200,
            params={"user_id": self.test_user}
        )
        if success:
            arm = response.get('arm')
            experiment_id = response.get('experiment_id')
            print(f"   User assigned to arm: {arm} in experiment: {experiment_id}")
        return success

    def test_popular_items(self):
        """Test popular items endpoint"""
        success, response = self.run_test(
            "Popular Items",
            "GET",
            "popular",
            200,
            params={"limit": 10}
        )
        if success:
            items = response.get('items', [])
            print(f"   Got {len(items)} popular items")
        return success

    def test_categories(self):
        """Test categories endpoint"""
        success, response = self.run_test(
            "Categories",
            "GET",
            "categories",
            200
        )
        if success:
            categories = response
            content_types = list(categories.keys()) if isinstance(categories, dict) else []
            print(f"   Found categories for content types: {content_types}")
        return success

    def test_stats(self):
        """Test system statistics"""
        success, response = self.run_test(
            "System Statistics",
            "GET",
            "stats",
            200
        )
        if success:
            stats = response.get('statistics', {})
            print(f"   Stats keys: {list(stats.keys()) if isinstance(stats, dict) else 'No stats'}")
        return success

    def test_content_type_filtering(self):
        """Test content type filtering in recommendations"""
        success, response = self.run_test(
            "Recommendations with Content Type Filter",
            "GET",
            "recommend",
            200,
            params={
                "user_id": self.test_user,
                "n": 5,
                "content_type": "course"
            }
        )
        if success:
            recommendations = response.get('recommendations', [])
            print(f"   Got {len(recommendations)} course recommendations")
            # Check if all results are courses
            course_count = sum(1 for rec in recommendations if rec.get('content_type') == 'course')
            print(f"   {course_count}/{len(recommendations)} are actually courses")
        return success

def main():
    print("🚀 Starting Recommendation System API Tests")
    print("=" * 60)
    
    tester = RecommendationSystemTester()
    
    # Test sequence
    tests = [
        ("Health Check", tester.test_health_check),
        ("Recommendations", tester.test_recommendations),
        ("Simple Search", tester.test_simple_search),
        ("AI Search", tester.test_ai_search),
        ("Event Logging", tester.test_log_event),
        ("A/B Testing", tester.test_ab_testing),
        ("Popular Items", tester.test_popular_items),
        ("Categories", tester.test_categories),
        ("System Stats", tester.test_stats),
        ("Content Type Filtering", tester.test_content_type_filtering)
    ]
    
    print(f"\nRunning {len(tests)} test suites...")
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            test_func()
        except Exception as e:
            print(f"❌ Test suite {test_name} failed with exception: {e}")
        
        # Small delay between tests
        time.sleep(1)
    
    # Print final results
    print(f"\n{'='*60}")
    print(f"📊 FINAL RESULTS")
    print(f"{'='*60}")
    print(f"Tests Run: {tester.tests_run}")
    print(f"Tests Passed: {tester.tests_passed}")
    print(f"Tests Failed: {tester.tests_run - tester.tests_passed}")
    print(f"Success Rate: {(tester.tests_passed/tester.tests_run*100):.1f}%" if tester.tests_run > 0 else "No tests run")
    
    if tester.tests_passed == tester.tests_run:
        print("🎉 All tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed. Check the logs above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())