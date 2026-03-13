#!/usr/bin/env python3
"""
チャット API テストスクリプト - 3ヶ国語の応答を確認

使い方:
    python test_chat.py https://xxx.execute-api.ap-northeast-1.amazonaws.com/prod

出力: 各言語でのテスト結果と、引用が正しく含まれているかの確認
"""
import sys
import json
import urllib.request
import urllib.error

# テスト質問（各言語）
TEST_QUESTIONS = {
    "en": "What is the Divine Principle?",
    "ja": "統一原理とは何ですか？",
    "ko": "통일원리란 무엇입니까?",
}

def print_header(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def test_chat(api_endpoint: str, language: str, question: str) -> dict:
    """チャット API をテスト"""
    url = f"{api_endpoint.rstrip('/')}/chat"
    
    data = json.dumps({
        "message": question,
        "language": language,
    }).encode("utf-8")
    
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}

def main():
    if len(sys.argv) < 2:
        print("使い方: python test_chat.py <API_ENDPOINT_URL>")
        print("例: python test_chat.py https://xxx.execute-api.ap-northeast-1.amazonaws.com/prod")
        return 1
    
    api_endpoint = sys.argv[1]
    
    print_header("Mini-Han チャット API テスト")
    print(f"エンドポイント: {api_endpoint}")
    
    results = {}
    
    for lang, question in TEST_QUESTIONS.items():
        lang_name = {"en": "English", "ja": "日本語", "ko": "한국어"}[lang]
        
        print_header(f"テスト: {lang_name} ({lang})")
        print(f"質問: {question}")
        print("\n応答を待っています...")
        
        result = test_chat(api_endpoint, lang, question)
        results[lang] = result
        
        if "error" in result:
            print(f"\n❌ エラー: {result['error']}")
        else:
            print(f"\n✅ 応答を受信しました")
            print(f"\n【Mini-Han の回答】")
            print("-" * 40)
            # 回答を短く表示（最大500文字）
            reply = result.get("reply", "")
            if len(reply) > 500:
                print(reply[:500] + "...")
            else:
                print(reply)
            print("-" * 40)
            
            # 出典確認
            sources = result.get("sources", [])
            if sources:
                print(f"\n📚 出典: {len(sources)} 件")
                for src in sources[:3]:  # 最大3件表示
                    print(f"   [{src['index']}] {src['filename']} ({src['language']})")
            else:
                print("\n⚠️ 出典がありません")
            
            # 言語チェック
            response_lang = result.get("language", "")
            if response_lang == lang:
                print(f"\n✅ 応答言語: {response_lang} (正しい)")
            else:
                print(f"\n⚠️ 応答言語: {response_lang} (期待: {lang})")
    
    # --------------------------------------------------
    # サマリー
    # --------------------------------------------------
    print_header("テスト結果サマリー")
    
    all_passed = True
    for lang, result in results.items():
        lang_name = {"en": "English", "ja": "日本語", "ko": "한국어"}[lang]
        
        if "error" in result:
            print(f"❌ {lang_name}: 失敗 - {result['error']}")
            all_passed = False
        elif result.get("language") != lang:
            print(f"⚠️ {lang_name}: 言語が一致しない")
            all_passed = False
        elif not result.get("sources"):
            print(f"⚠️ {lang_name}: 出典なし")
        else:
            print(f"✅ {lang_name}: 成功")
    
    if all_passed:
        print("\n🎉 全てのテストに成功しました！")
    else:
        print("\n⚠️ 一部のテストに問題があります。上記を確認してください。")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
