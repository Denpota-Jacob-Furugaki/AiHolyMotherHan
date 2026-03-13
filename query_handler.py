"""
Hybrid Query Handler for AI Holy Mother Han
Handles both theological queries and general conversational questions
"""

import json
from pathlib import Path
from openai import OpenAI
import os

class QueryHandler:
    def __init__(self, collection_path, glossary_path, citation_index_path=None):
        self.collection_path = Path(collection_path)
        self.glossary_path = Path(glossary_path)
        self.citation_index_path = Path(citation_index_path) if citation_index_path else None
        self.client = OpenAI()
        
        # Load theological glossary
        with open(self.glossary_path, 'r', encoding='utf-8') as f:
            self.glossary = json.load(f)
        
        # Load citation index if available
        self.citations = []
        if self.citation_index_path and self.citation_index_path.exists():
            with open(self.citation_index_path, 'r', encoding='utf-8') as f:
                self.citations = json.load(f)
    
    def classify_query(self, query):
        """
        Classify whether query is theological or general.
        Returns: 'theological', 'general', or 'hybrid'
        """
        # Theological keywords (multilingual)
        theological_keywords = {
            'en': ['god', 'jesus', 'messiah', 'true parents', 'divine principle', 
                   'blessing', 'restoration', 'indemnity', 'providence', 'fall',
                   'spiritual world', 'heaven', 'hell', 'salvation', 'prayer'],
            'ko': ['하나님', '예수', '메시아', '참부모', '원리', '축복', '복귀', 
                   '탕감', '섭리', '타락', '영계', '천국', '지옥', '구원', '기도',
                   '참사랑', '참가정', '천일국'],
            'ja': ['神様', 'イエス', 'メシア', '真の父母', '原理', '祝福', '復帰',
                   '蕩減', '摂理', '堕落', '霊界', '天国', '地獄', '救援', '祈り',
                   '真の愛', '真の家庭', '天一国']
        }
        
        query_lower = query.lower()
        
        # Check for theological keywords
        theological_match = False
        for lang_keywords in theological_keywords.values():
            if any(keyword in query_lower for keyword in lang_keywords):
                theological_match = True
                break
        
        # Check for general life questions
        general_patterns = {
            'en': ['how to', 'how can i', 'advice', 'help me', 'relationship', 
                   'work', 'boss', 'family problem', 'stress', 'anxiety'],
            'ko': ['어떻게', '방법', '조언', '도와주', '관계', '직장', '상사', 
                   '가족 문제', '스트레스', '불안'],
            'ja': ['どうやって', '方法', 'アドバイス', '助けて', '関係', '職場',
                   '上司', '家族問題', 'ストレス', '不安', '乗り越える']
        }
        
        general_match = False
        for lang_patterns in general_patterns.values():
            if any(pattern in query_lower for pattern in lang_patterns):
                general_match = True
                break
        
        if theological_match and general_match:
            return 'hybrid'
        elif theological_match:
            return 'theological'
        else:
            return 'general'
    
    def search_teachings(self, query, max_results=5):
        """
        Search True Parents' Word Collection for relevant teachings.
        Returns list of relevant passages with citations.
        """
        # Expand query with related theological terms
        expanded_keywords = self._expand_query_keywords(query)
        
        results = []
        scored_results = []
        
        # Search through text files
        for txt_file in sorted(self.collection_path.glob("*.txt")):
            try:
                with open(txt_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    
                    # Search for relevant passages
                    for i, line in enumerate(lines):
                        # Calculate relevance score
                        score = 0
                        line_lower = line.lower()
                        
                        # Check for query keywords
                        for keyword in expanded_keywords:
                            if keyword in line_lower:
                                score += 2
                        
                        # Bonus for meaningful content (not just headers)
                        if len(line.strip()) > 50:
                            score += 1
                        
                        if score > 0:
                            # Get context (10 lines before and after for better understanding)
                            start = max(0, i - 10)
                            end = min(len(lines), i + 11)
                            context_lines = lines[start:end]
                            
                            # Clean up the snippet
                            snippet = '\n'.join(context_lines)
                            snippet = snippet.strip()
                            
                            # Limit snippet length but try to keep complete sentences
                            if len(snippet) > 800:
                                snippet = snippet[:800] + "..."
                            
                            scored_results.append({
                                'file': txt_file.name,
                                'volume': txt_file.stem,
                                'snippet': snippet,
                                'line': i,
                                'score': score
                            })
            except Exception as e:
                continue
        
        # Sort by score and return top results
        scored_results.sort(key=lambda x: x['score'], reverse=True)
        return scored_results[:max_results]
    
    def _expand_query_keywords(self, query):
        """
        Expand query with related theological terms and synonyms.
        """
        keywords = set(query.lower().split())
        
        # Map common themes to theological concepts
        theme_mappings = {
            # Relationships
            'relationship': ['love', 'heart', 'unity', 'harmony'],
            '関係': ['愛', '心情', '一体', '調和'],
            'boss': ['authority', 'leadership', 'respect'],
            '上司': ['権威', 'リーダーシップ', '尊敬'],
            
            # Challenges
            'overcome': ['victory', 'triumph', 'perseverance'],
            '乗り越える': ['勝利', '克服', '忍耐'],
            'difficult': ['challenge', 'trial', 'suffering'],
            '難しい': ['試練', '苦難', '困難'],
            
            # Family
            'family': ['true family', 'parents', 'children'],
            '家族': ['真の家庭', '父母', '子女'],
            
            # Spiritual
            'god': ['heavenly father', 'creator', 'divine'],
            '神': ['天の父母', '創造主', '神様'],
        }
        
        # Add related terms
        for key, related in theme_mappings.items():
            if key in query.lower():
                keywords.update(related)
        
        return list(keywords)
    
    def generate_response(self, query, query_type, search_results=None):
        """
        Generate response using OpenAI with appropriate context.
        Creates encouraging, solution-oriented responses with relevant quotes.
        """
        # Build system prompt based on query type
        base_instruction = """IMPORTANT RESPONSE FORMAT:
1. Start with empathy and understanding
2. Quote relevant teachings from True Parents (if available) with proper citation
3. Provide practical, actionable advice
4. End with encouragement

When quoting, use this format:
「引用文」
— 真の父母様のみ言葉選集 第○巻

Or in English:
"Quote text"
— True Parents' Word Collection, Vol. X

Speak naturally in the user's language. Be warm, encouraging, and solution-focused."""

        if query_type == 'theological':
            system_prompt = f"""You are AI Holy Mother Han, an expert on True Parents' teachings and the Divine Principle.

{base_instruction}

Answer theological questions based on:
1. The Divine Principle
2. True Parents' Word Collection
3. Unification theology

Use the theological glossary for consistent terminology:
{json.dumps(self.glossary['terms'], ensure_ascii=False, indent=2)[:1000]}

Provide accurate, reverent answers grounded in the teachings."""

        elif query_type == 'general':
            system_prompt = f"""You are AI Holy Mother Han, a compassionate advisor who helps people with life challenges.

{base_instruction}

For general questions:
1. Acknowledge their struggle with empathy
2. If relevant teachings are provided, quote them to offer spiritual perspective
3. Provide practical, actionable steps they can take
4. Encourage them with hope and confidence

While you have deep knowledge of True Parents' teachings, you can also provide general life advice.
Speak naturally in the user's language (Japanese, Korean, or English).

You are here to help people live better lives."""

        else:  # hybrid
            system_prompt = f"""You are AI Holy Mother Han, combining deep theological knowledge with practical life wisdom.

{base_instruction}

Answer questions by:
1. Acknowledging their concern with empathy
2. Quoting relevant True Parents' teachings (if available)
3. Connecting spiritual wisdom to practical action
4. Providing concrete steps they can take
5. Ending with encouragement

Theological glossary:
{json.dumps(self.glossary['terms'], ensure_ascii=False, indent=2)[:1000]}

Be both practical and spiritually grounded."""

        # Format search results as citations
        context = ""
        if search_results and len(search_results) > 0:
            context = "\n\n【参考となる真の父母様のみ言葉】\n"
            context += "Use these teachings to support your answer. Quote the most relevant parts.\n\n"
            for idx, result in enumerate(search_results, 1):
                context += f"\n[引用 {idx}] 真の父母様のみ言葉選集 第{result['volume']}巻:\n"
                context += f"{result['snippet']}\n"
                context += f"(関連度スコア: {result.get('score', 0)})\n"
        
        # Generate response
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"{query}\n{context}"}
                ],
                temperature=0.7,
                max_tokens=1500
            )
            
            answer = response.choices[0].message.content
            
            # Format citations at the end if not already included
            citations = []
            if search_results:
                for result in search_results:
                    citations.append({
                        'volume': result['volume'],
                        'file': result['file']
                    })
            
            return {
                'success': True,
                'answer': answer,
                'query_type': query_type,
                'sources': search_results if search_results else [],
                'citations': citations
            }
        
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'query_type': query_type
            }
    
    def handle_query(self, query):
        """
        Main entry point: classify query and generate appropriate response.
        """
        # Classify the query
        query_type = self.classify_query(query)
        
        # Search for relevant teachings if theological or hybrid
        search_results = None
        if query_type in ['theological', 'hybrid']:
            search_results = self.search_teachings(query)
        
        # Generate response
        response = self.generate_response(query, query_type, search_results)
        
        return response


def main():
    """Test the query handler"""
    collection_path = r"C:\Users\denpo\OneDrive\Coding\AIHolyMotherHan\Translation\EN True Parents' Word Collection"
    glossary_path = r"C:\Users\denpo\OneDrive\Coding\AIHolyMotherHan\Translation\theological_glossary.json"
    citation_index = r"C:\Users\denpo\OneDrive\Coding\AIHolyMotherHan\Translation\citation_index.json"
    
    handler = QueryHandler(collection_path, glossary_path, citation_index)
    
    # Test queries
    test_queries = [
        "嫌いな上司との関係を乗り越える方法を知りたい",  # General
        "What is the Divine Principle?",  # Theological
        "How can I build a True Family?",  # Hybrid
        "하나님의 심정은 무엇입니까?",  # Theological (Korean)
    ]
    
    print("="*80)
    print("Query Handler Test")
    print("="*80)
    
    for query in test_queries:
        print(f"\n\nQuery: {query}")
        print("-"*80)
        
        result = handler.handle_query(query)
        
        if result['success']:
            print(f"Type: {result['query_type']}")
            print(f"\nAnswer:\n{result['answer']}")
            
            if result['sources']:
                print(f"\nSources: {len(result['sources'])} relevant passages found")
        else:
            print(f"Error: {result['error']}")


if __name__ == "__main__":
    # Check for API key
    if not os.getenv('OPENAI_API_KEY'):
        print("Please set OPENAI_API_KEY environment variable")
    else:
        main()
