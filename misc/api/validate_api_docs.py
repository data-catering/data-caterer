#!/usr/bin/env python3
"""
API Documentation Validation Script for Data Caterer

This script validates that the API documentation (docs/docs/api.md) is aligned 
with the actual PlanRoutes.scala implementation by:
1. Parsing the Scala routes to extract endpoint definitions
2. Parsing the API documentation to extract documented endpoints  
3. Comparing them to identify discrepancies
4. Generating a validation report

The script focuses on REST API endpoints and excludes UI-serving endpoints 
that return HTML pages (/, /connection, /plan, /history).

Usage: 
    python3 misc/api/validate_api_docs.py
    ./misc/api/validate_api_docs.py

Output:
    - Console report showing validation results
    - api_validation_report.txt with detailed findings
    
Exit codes:
    0 - All endpoints are properly documented (PASS)
    1 - Issues found that need attention (FAIL)
"""

import re
import os
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Endpoint:
    """Represents an API endpoint with its properties"""
    method: str
    path: str
    description: str = ""
    request_body: bool = False
    path_params: List[str] = None
    query_params: List[str] = None
    
    def __post_init__(self):
        if self.path_params is None:
            self.path_params = []
        if self.query_params is None:
            self.query_params = []
    
    def __str__(self):
        return f"{self.method} {self.path}"
    
    def __eq__(self, other):
        if not isinstance(other, Endpoint):
            return False
        return self.method == other.method and self.normalize_path(self.path) == self.normalize_path(other.path)
    
    def __hash__(self):
        return hash((self.method, self.normalize_path(self.path)))
    
    @staticmethod
    def normalize_path(path: str) -> str:
        """Normalize path for comparison by replacing path parameters with placeholders"""
        # Replace regex patterns with {param}
        path = re.sub(r'"""[^"]*"""\.r', '{param}', path)
        # Replace Segments and Remaining with placeholders
        path = re.sub(r'Segments\([^)]+\)', '{segments}', path)
        path = re.sub(r'Remaining', '{remaining}', path)
        # Clean up extra slashes and normalize
        path = re.sub(r'/+', '/', path)
        return path.strip('/')

class ScalaRoutesParser:
    """Parser for extracting endpoints from PlanRoutes.scala"""
    
    def __init__(self, routes_file_path: str):
        self.routes_file_path = routes_file_path
        self.endpoints: List[Endpoint] = []
    
    def parse(self) -> List[Endpoint]:
        """Parse the Scala routes file and extract endpoints"""
        try:
            with open(self.routes_file_path, 'r') as f:
                content = f.read()
            
            # Use a simpler approach - look for specific patterns we know exist
            self._parse_specific_patterns(content)
            return self.endpoints
        except FileNotFoundError:
            print(f"Error: Could not find routes file at {self.routes_file_path}")
            return []
        except Exception as e:
            print(f"Error parsing routes file: {e}")
            return []
    
    def _parse_specific_patterns(self, content: str):
        """Parse specific endpoint patterns we can identify"""
        # Define known API endpoints from the file (excluding UI-serving endpoints)
        endpoint_patterns = [
            # Note: Excluding UI-serving endpoints that return HTML pages:
            # - GET / (serves index.html)
            # - GET /connection (serves connection.html) 
            # - GET /plan (serves plan.html)
            # - GET /history (serves history.html)
            # These are not REST API endpoints
            
            # pathPrefix patterns
            (r'pathPrefix\("run"\).*?path\("delete-data"\)\s*\{\s*post.*?entity\(as\[', 'POST', '/run/delete-data', True),
            (r'pathPrefix\("run"\).*?path\("history"\)\s*\{\s*get', 'GET', '/run/history'),
            (r'pathPrefix\("run"\).*?path\("status".*?get', 'GET', '/run/status/{id}'),
            (r'pathPrefix\("run"\).*?post.*?entity\(as\[', 'POST', '/run', True),
            
            # Connection endpoints
            (r'pathPrefix\("connection"\).*?post.*?entity\(as\[', 'POST', '/connection', True),
            (r'pathPrefix\("connection"\).*?get.*?rejectEmptyResponse', 'GET', '/connection/{connectionName}'),
            (r'pathPrefix\("connection"\).*?delete', 'DELETE', '/connection/{connectionName}'),
            
            # Connections endpoint
            (r'pathPrefix\("connections"\).*?parameter.*?get', 'GET', '/connections'),
            
            # Plan endpoints
            (r'pathPrefix\("plan"\).*?post.*?entity\(as\[', 'POST', '/plan', True),
            (r'pathPrefix\("plan"\).*?get.*?rejectEmptyResponse', 'GET', '/plan/{planName}'),
            (r'pathPrefix\("plan"\).*?delete', 'DELETE', '/plan/{planName}'),
            
            # Plans endpoint
            (r'pathPrefix\("plans"\)', 'GET', '/plans'),
            
            # Report endpoint
            (r'pathPrefix\("report".*?get', 'GET', '/report/{runId}/{resource}'),
            
            # Sample endpoints
            (r'pathPrefix\("sample"\).*?path\("task-file"\).*?post.*?entity\(as\[', 'POST', '/sample/task-file', True),
            (r'pathPrefix\("sample"\).*?path\("schema"\).*?post.*?entity\(as\[', 'POST', '/sample/schema', True),
            
            # Shutdown
            (r'path\("shutdown"\).*?system\.terminate', 'POST', '/shutdown'),
        ]
        
        for pattern_info in endpoint_patterns:
            if len(pattern_info) == 3:
                pattern, method, path = pattern_info
                has_request_body = False
            else:
                pattern, method, path, has_request_body = pattern_info
            
            if re.search(pattern, content, re.DOTALL):
                # Extract path parameters
                path_params = re.findall(r'\{([^}]+)\}', path)
                
                # Look for query parameters in the pattern
                query_params = []
                if 'parameter' in pattern:
                    # Find the actual parameter name from the content
                    param_match = re.search(r'parameter\("([^"]+)"', content)
                    if param_match:
                        query_params.append(param_match.group(1))
                
                endpoint = Endpoint(
                    method=method,
                    path=path,
                    request_body=has_request_body,
                    path_params=path_params,
                    query_params=query_params
                )
                
                self.endpoints.append(endpoint)

class ApiDocsParser:
    """Parser for extracting endpoints from API documentation"""
    
    def __init__(self, docs_file_path: str):
        self.docs_file_path = docs_file_path
        self.endpoints: List[Endpoint] = []
    
    def parse(self) -> List[Endpoint]:
        """Parse the API documentation and extract endpoints"""
        try:
            with open(self.docs_file_path, 'r') as f:
                content = f.read()
            
            self._parse_endpoints(content)
            return self.endpoints
        except FileNotFoundError:
            print(f"Error: Could not find documentation file at {self.docs_file_path}")
            return []
        except Exception as e:
            print(f"Error parsing documentation file: {e}")
            return []
    
    def _parse_endpoints(self, content: str):
        """Parse endpoint definitions from markdown content"""
        # Pattern to match endpoint definitions
        endpoint_pattern = r'\*\*Endpoint:\*\*\s*`(\w+)\s+([^`]+)`'
        
        for match in re.finditer(endpoint_pattern, content):
            method = match.group(1)
            path = match.group(2)
            
            # Extract path parameters
            path_params = re.findall(r'\{([^}]+)\}', path)
            
            # Look for query parameters in the surrounding text
            # Find the section containing this endpoint
            start_pos = match.start()
            next_endpoint = re.search(endpoint_pattern, content[start_pos + 1:])
            end_pos = next_endpoint.start() + start_pos + 1 if next_endpoint else len(content)
            
            section_content = content[start_pos:end_pos]
            
            # Extract query parameters
            query_params = []
            query_param_matches = re.findall(r'`([^`]+)`\s*\(optional\)', section_content)
            query_params.extend(query_param_matches)
            
            # Check for request body
            has_request_body = bool(re.search(r'\*\*Request Body:\*\*', section_content))
            
            # Extract description (look for the heading before this endpoint)
            desc_match = re.search(r'###\s*([^\n]+)\n[^#]*?\*\*Endpoint:\*\*', content[:match.end()], re.DOTALL)
            description = desc_match.group(1).strip() if desc_match else ""
            
            endpoint = Endpoint(
                method=method,
                path=path,
                description=description,
                request_body=has_request_body,
                path_params=path_params,
                query_params=query_params
            )
            
            self.endpoints.append(endpoint)

class ApiValidator:
    """Validates API documentation against actual implementation"""
    
    def __init__(self, routes_file: str, docs_file: str):
        self.routes_file = routes_file
        self.docs_file = docs_file
        self.scala_parser = ScalaRoutesParser(routes_file)
        self.docs_parser = ApiDocsParser(docs_file)
    
    def validate(self) -> Dict[str, any]:
        """Perform validation and return results"""
        print("🔍 Parsing PlanRoutes.scala...")
        scala_endpoints = self.scala_parser.parse()
        
        print("📖 Parsing API documentation...")
        docs_endpoints = self.docs_parser.parse()
        
        print("⚖️  Comparing endpoints...")
        
        # Convert to sets for comparison
        scala_set = set(scala_endpoints)
        docs_set = set(docs_endpoints)
        
        # Find discrepancies
        missing_in_docs = scala_set - docs_set
        extra_in_docs = docs_set - scala_set
        common_endpoints = scala_set & docs_set
        
        # Detailed comparison for common endpoints
        detailed_issues = []
        for scala_ep in scala_endpoints:
            for docs_ep in docs_endpoints:
                if scala_ep == docs_ep:
                    issues = self._compare_endpoint_details(scala_ep, docs_ep)
                    if issues:
                        detailed_issues.extend(issues)
        
        return {
            'scala_endpoints': scala_endpoints,
            'docs_endpoints': docs_endpoints,
            'missing_in_docs': list(missing_in_docs),
            'extra_in_docs': list(extra_in_docs),
            'common_endpoints': list(common_endpoints),
            'detailed_issues': detailed_issues,
            'total_scala': len(scala_endpoints),
            'total_docs': len(docs_endpoints),
            'coverage_percentage': len(common_endpoints) / len(scala_endpoints) * 100 if scala_endpoints else 0
        }
    
    def _compare_endpoint_details(self, scala_ep: Endpoint, docs_ep: Endpoint) -> List[str]:
        """Compare detailed properties of matching endpoints"""
        issues = []
        
        if scala_ep.request_body != docs_ep.request_body:
            issues.append(f"{scala_ep}: Request body mismatch - Scala: {scala_ep.request_body}, Docs: {docs_ep.request_body}")
        
        if set(scala_ep.path_params) != set(docs_ep.path_params):
            issues.append(f"{scala_ep}: Path parameters mismatch - Scala: {scala_ep.path_params}, Docs: {docs_ep.path_params}")
        
        if set(scala_ep.query_params) != set(docs_ep.query_params):
            issues.append(f"{scala_ep}: Query parameters mismatch - Scala: {scala_ep.query_params}, Docs: {docs_ep.query_params}")
        
        return issues
    
    def generate_report(self, results: Dict[str, any]) -> str:
        """Generate a human-readable validation report"""
        report = []
        report.append("=" * 60)
        report.append("🔍 API DOCUMENTATION VALIDATION REPORT")
        report.append("=" * 60)
        report.append("")
        
        # Summary
        report.append("📊 SUMMARY")
        report.append("-" * 20)
        report.append(f"Scala endpoints found: {results['total_scala']}")
        report.append(f"Documented endpoints: {results['total_docs']}")
        report.append(f"Coverage: {results['coverage_percentage']:.1f}%")
        report.append(f"Common endpoints: {len(results['common_endpoints'])}")
        report.append(f"Missing from docs: {len(results['missing_in_docs'])}")
        report.append(f"Extra in docs: {len(results['extra_in_docs'])}")
        report.append(f"Detail issues: {len(results['detailed_issues'])}")
        report.append("")
        
        # Missing endpoints
        if results['missing_in_docs']:
            report.append("❌ ENDPOINTS MISSING FROM DOCUMENTATION")
            report.append("-" * 40)
            for endpoint in results['missing_in_docs']:
                report.append(f"  • {endpoint}")
                if endpoint.path_params:
                    report.append(f"    Path params: {endpoint.path_params}")
                if endpoint.query_params:
                    report.append(f"    Query params: {endpoint.query_params}")
                if endpoint.request_body:
                    report.append(f"    Has request body: Yes")
            report.append("")
        
        # Extra endpoints
        if results['extra_in_docs']:
            report.append("⚠️  ENDPOINTS DOCUMENTED BUT NOT FOUND IN CODE")
            report.append("-" * 45)
            for endpoint in results['extra_in_docs']:
                report.append(f"  • {endpoint}")
            report.append("")
        
        # Detail issues
        if results['detailed_issues']:
            report.append("🔧 ENDPOINT DETAIL MISMATCHES")
            report.append("-" * 30)
            for issue in results['detailed_issues']:
                report.append(f"  • {issue}")
            report.append("")
        
        # Correctly documented endpoints
        if results['common_endpoints']:
            report.append("✅ CORRECTLY DOCUMENTED ENDPOINTS")
            report.append("-" * 35)
            for endpoint in sorted(results['common_endpoints'], key=lambda x: (x.path, x.method)):
                report.append(f"  • {endpoint}")
            report.append("")
        
        # All Scala endpoints for reference
        report.append("📋 ALL SCALA ENDPOINTS (Reference)")
        report.append("-" * 35)
        for endpoint in sorted(results['scala_endpoints'], key=lambda x: (x.path, x.method)):
            report.append(f"  • {endpoint}")
            if endpoint.path_params:
                report.append(f"    Path params: {endpoint.path_params}")
            if endpoint.query_params:
                report.append(f"    Query params: {endpoint.query_params}")
            if endpoint.request_body:
                report.append(f"    Request body: Yes")
        report.append("")
        
        # Validation status
        is_valid = (len(results['missing_in_docs']) == 0 and 
                   len(results['extra_in_docs']) == 0 and 
                   len(results['detailed_issues']) == 0)
        
        if is_valid:
            report.append("🎉 VALIDATION RESULT: PASS")
            report.append("Documentation is fully aligned with implementation!")
        else:
            report.append("❗ VALIDATION RESULT: ISSUES FOUND")
            report.append("Please review and fix the issues listed above.")
        
        report.append("")
        report.append("=" * 60)
        
        return "\n".join(report)

def main():
    """Main function to run the validation"""
    # File paths - script is now in misc/api/ folder, so go up two levels
    base_dir = Path(__file__).parent.parent.parent
    routes_file = base_dir / "app/src/main/scala/io/github/datacatering/datacaterer/core/ui/plan/PlanRoutes.scala"
    docs_file = base_dir / "docs/docs/api.md"
    
    # Check if files exist
    if not routes_file.exists():
        print(f"❌ Error: PlanRoutes.scala not found at {routes_file}")
        return 1
    
    if not docs_file.exists():
        print(f"❌ Error: API documentation not found at {docs_file}")
        return 1
    
    print("🚀 Starting API documentation validation...")
    print(f"📁 Routes file: {routes_file}")
    print(f"📁 Docs file: {docs_file}")
    print()
    
    # Run validation
    validator = ApiValidator(str(routes_file), str(docs_file))
    results = validator.validate()
    
    # Generate and print report
    report = validator.generate_report(results)
    print(report)
    
    # Save report to file
    report_file = base_dir / "api_validation_report.txt"
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"📄 Full report saved to: {report_file}")
    
    # Return appropriate exit code
    has_issues = (len(results['missing_in_docs']) > 0 or 
                 len(results['extra_in_docs']) > 0 or 
                 len(results['detailed_issues']) > 0)
    
    return 1 if has_issues else 0

if __name__ == "__main__":
    exit(main())
