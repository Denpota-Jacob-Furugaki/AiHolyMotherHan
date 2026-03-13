"""
AWS Elastic Beanstalk compatible entry point
Rename from api_server.py for EB deployment
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import os
from pathlib import Path

# Query handler is in the same directory
from query_handler import QueryHandler

application = Flask(__name__)
CORS(application, resources={r"/api/*": {"origins": "*"}})  # Allow all origins for API

# Initialize query handler - use local copies
COLLECTION_PATH = Path(__file__).parent / 'speeches'
GLOSSARY_PATH = Path(__file__).parent / 'theological_glossary.json'
CITATION_INDEX = Path(__file__).parent / 'citation_index.json'

query_handler = None

def init_query_handler():
    """Initialize the query handler"""
    global query_handler
    try:
        query_handler = QueryHandler(
            str(COLLECTION_PATH),
            str(GLOSSARY_PATH),
            str(CITATION_INDEX) if CITATION_INDEX.exists() else None
        )
        print("✅ Query handler initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Error initializing query handler: {e}")
        return False

@application.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return jsonify({
        'service': 'AI Holy Mother Han API',
        'status': 'running',
        'version': '1.0.0',
        'endpoints': {
            'query': 'POST /api/query',
            'search': 'POST /api/search',
            'citations': 'GET /api/citations',
            'health': 'GET /health'
        }
    })

@application.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'handler_initialized': query_handler is not None
    })

@application.route('/api/query', methods=['POST', 'OPTIONS'])
def handle_query():
    """Handle user queries with CORS support"""
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        data = request.json
        query = data.get('query', '')
        language = data.get('language', 'ja')
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Query is required'
            }), 400
        
        if not query_handler:
            if not init_query_handler():
                return jsonify({
                    'success': False,
                    'error': 'Query handler not initialized'
                }), 500
        
        # Process the query
        result = query_handler.handle_query(query)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@application.route('/api/search', methods=['POST', 'OPTIONS'])
def search_teachings():
    """Search for specific teachings"""
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        data = request.json
        query = data.get('query', '')
        max_results = data.get('max_results', 5)
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Query is required'
            }), 400
        
        if not query_handler:
            if not init_query_handler():
                return jsonify({
                    'success': False,
                    'error': 'Query handler not initialized'
                }), 500
        
        # Search teachings
        results = query_handler.search_teachings(query, max_results)
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@application.route('/api/citations', methods=['GET'])
def get_citations():
    """Get available citations"""
    try:
        if CITATION_INDEX.exists():
            import json
            with open(CITATION_INDEX, 'r', encoding='utf-8') as f:
                citations = json.load(f)
            return jsonify({
                'success': True,
                'citations': citations[:100],
                'total': len(citations)
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Citation index not yet generated'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Initialize on startup
init_query_handler()

if __name__ == '__main__':
    # For local development
    port = int(os.environ.get('PORT', 5000))
    application.run(host='0.0.0.0', port=port, debug=False)
