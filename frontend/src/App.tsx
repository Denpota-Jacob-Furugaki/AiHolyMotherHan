import { useState, useRef, useEffect, useCallback } from 'react'
import { GoogleLogin, googleLogout } from '@react-oauth/google'
import './App.css'

// ============================================================
// 型定義
// ============================================================

type Language = 'en' | 'ja' | 'ko'

interface Source {
  index: number
  filename: string
  s3_key: string
  language: string
  similarity: number
  excerpt: string
}

interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
  sources?: Source[]
  language?: Language
}

interface ChatResponse {
  reply: string
  sources: Source[]
  language: Language
  queries_used?: number
  queries_remaining?: number
  daily_limit?: number
  unlimited?: boolean
}

interface GoogleUser {
  sub: string
  name: string
  picture: string
  idToken: string
}

// ============================================================
// Constants
// ============================================================

const API_ENDPOINT = 'https://1o0165j9ig.execute-api.ap-northeast-1.amazonaws.com/prod'
const STORAGE_LANG = 'mini-han-language'
const STORAGE_USER = 'mini-han-google-user'

// ============================================================
// 多言語テキスト
// ============================================================

const TEXTS = {
  en: {
    title: 'AI Holy Mother Han',
    subtitle: 'Ask anything about True Parents',
    greeting: "Hello! 👋 I'm Mini-Han. Sign in with Google to start asking questions about True Parents!",
    greetingSignedIn: (name: string) => `Hello, ${name}! 👋 Ask me anything about True Parents.`,
    placeholder: 'Type your question...',
    send: 'Send',
    sources: 'Sources',
    sending: 'Thinking...',
    signIn: 'Sign in with Google to start',
    signOut: 'Sign out',
    queriesLeft: (n: number) => `${n} questions left today`,
    unlimited: 'Unlimited',
    limitTitle: "You've reached today's limit",
    limitBody: 'You can ask 10 questions per day. Your limit resets at midnight JST.',
    limitClose: 'OK, come back tomorrow',
    couponBtn: 'Have a coupon?',
    couponTitle: 'Redeem Coupon Code',
    couponPlaceholder: 'Enter coupon code...',
    couponApply: 'Apply',
    couponApplying: 'Applying...',
    couponSuccess: '✅ Unlimited access activated!',
    couponInvalid: 'Invalid or already used coupon code.',
    couponError: 'Something went wrong. Try again.',
  },
  ja: {
    title: 'AI 聖母韓鶴子',
    subtitle: '真の御父母様について何でも聞いてください',
    greeting: 'こんにちは！👋 Googleでログインして、真の御父母様についての質問を始めましょう！',
    greetingSignedIn: (name: string) => `${name}さん、こんにちは！👋 真の御父母様について何でも聞いてください。`,
    placeholder: '質問を入力してください...',
    send: '送信',
    sources: '出典',
    sending: '考え中...',
    signIn: 'Googleでサインインして開始',
    signOut: 'サインアウト',
    queriesLeft: (n: number) => `本日残り ${n} 回`,
    unlimited: '無制限',
    limitTitle: '本日の質問上限に達しました',
    limitBody: '1日10回まで質問できます。日本時間の深夜0時にリセットされます。',
    limitClose: 'OK、また明日',
    couponBtn: 'クーポンをお持ちですか？',
    couponTitle: 'クーポンコードを入力',
    couponPlaceholder: 'クーポンコード...',
    couponApply: '適用',
    couponApplying: '確認中...',
    couponSuccess: '✅ 無制限アクセスが有効になりました！',
    couponInvalid: '無効または使用済みのクーポンコードです。',
    couponError: 'エラーが発生しました。もう一度お試しください。',
  },
  ko: {
    title: 'AI 성모 한학자',
    subtitle: '참부모님에 대해 무엇이든 물어보세요',
    greeting: '안녕하세요! 👋 Google로 로그인하여 참부모님에 대한 질문을 시작하세요!',
    greetingSignedIn: (name: string) => `${name}님, 안녕하세요! 👋 참부모님에 대해 무엇이든 물어보세요.`,
    placeholder: '질문을 입력하세요...',
    send: '보내기',
    sources: '출처',
    sending: '생각 중...',
    signIn: 'Google로 로그인하여 시작',
    signOut: '로그아웃',
    queriesLeft: (n: number) => `오늘 남은 질문 ${n}회`,
    unlimited: '무제한',
    limitTitle: '오늘의 질문 한도에 도달했습니다',
    limitBody: '하루에 10번 질문할 수 있습니다. 자정(JST)에 초기화됩니다.',
    limitClose: '확인, 내일 다시',
    couponBtn: '쿠폰 코드가 있으신가요?',
    couponTitle: '쿠폰 코드 입력',
    couponPlaceholder: '쿠폰 코드...',
    couponApply: '적용',
    couponApplying: '확인 중...',
    couponSuccess: '✅ 무제한 액세스가 활성화되었습니다!',
    couponInvalid: '유효하지 않거나 이미 사용된 쿠폰 코드입니다.',
    couponError: '오류가 발생했습니다. 다시 시도해 주세요.',
  },
}

// ============================================================
// App コンポーネント
// ============================================================

function App() {
  const [language, setLanguage] = useState<Language>(() =>
    (localStorage.getItem(STORAGE_LANG) as Language) || 'ja'
  )
  const [user, setUser] = useState<GoogleUser | null>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_USER)
      return stored ? JSON.parse(stored) : null
    } catch { return null }
  })
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [queriesRemaining, setQueriesRemaining] = useState<number | null>(null)
  const [isUnlimited, setIsUnlimited] = useState(false)
  const [showLimitModal, setShowLimitModal] = useState(false)
  const [showCouponModal, setShowCouponModal] = useState(false)
  const [couponInput, setCouponInput] = useState('')
  const [couponStatus, setCouponStatus] = useState<'idle' | 'loading' | 'success' | 'invalid' | 'error'>('idle')

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const t = TEXTS[language]

  // Set greeting on language or login state change
  useEffect(() => {
    localStorage.setItem(STORAGE_LANG, language)
    const greeting = user ? t.greetingSignedIn(user.name) : t.greeting
    setMessages([{ role: 'assistant', content: greeting }])
  }, [language, user]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const handleLoginSuccess = useCallback((credentialResponse: { credential?: string }) => {
    const idToken = credentialResponse.credential
    if (!idToken) return
    // Decode JWT payload (no verification needed on client)
    try {
      const payload = JSON.parse(atob(idToken.split('.')[1]))
      const googleUser: GoogleUser = {
        sub: payload.sub,
        name: payload.name || payload.email,
        picture: payload.picture || '',
        idToken,
      }
      setUser(googleUser)
      localStorage.setItem(STORAGE_USER, JSON.stringify(googleUser))
    } catch (e) {
      console.error('Failed to parse Google token', e)
    }
  }, [])

  const handleSignOut = useCallback(() => {
    googleLogout()
    setUser(null)
    setQueriesRemaining(null)
    setIsUnlimited(false)
    localStorage.removeItem(STORAGE_USER)
  }, [])

  const handleRedeemCoupon = async () => {
    if (!couponInput.trim() || !user) return
    setCouponStatus('loading')
    try {
      const res = await fetch(`${API_ENDPOINT}/redeem-coupon`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ google_id_token: user.idToken, coupon_code: couponInput.trim() }),
      })
      const data = await res.json()
      if (data.success) {
        setCouponStatus('success')
        setIsUnlimited(true)
        setTimeout(() => setShowCouponModal(false), 1800)
      } else if (data.message === 'invalid_coupon') {
        setCouponStatus('invalid')
      } else {
        setCouponStatus('error')
      }
    } catch {
      setCouponStatus('error')
    }
  }

  const handleSend = async () => {
    if (!input.trim() || isLoading || !user) return

    const userMessage = input.trim()
    setInput('')
    setMessages(prev => [...prev, { role: 'user', content: userMessage }])
    setIsLoading(true)

    try {
      const response = await fetch(`${API_ENDPOINT}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: userMessage,
          language,
          google_id_token: user.idToken,
        }),
      })

      if (response.status === 429) {
        setShowLimitModal(true)
        setQueriesRemaining(0)
        setMessages(prev => prev.slice(0, -1))
        setInput(userMessage)
        return
      }

      if (response.status === 401) {
        // Token expired — sign out and ask to re-login
        handleSignOut()
        setMessages(prev => [...prev, { role: 'assistant', content: '⚠️ Session expired. Please sign in again.' }])
        return
      }

      if (!response.ok) {
        const err = await response.json().catch(() => ({ error: response.statusText }))
        throw new Error(err.detail || err.error || `HTTP ${response.status}`)
      }

      const data: ChatResponse = await response.json()
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: data.reply, sources: data.sources, language: data.language },
      ])

      if (data.unlimited) {
        setIsUnlimited(true)
      }
      if (data.queries_remaining !== undefined && !data.unlimited) {
        setQueriesRemaining(data.queries_remaining)
        if (data.queries_remaining === 0) {
          setTimeout(() => setShowLimitModal(true), 800)
        }
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error)
      setMessages(prev => [...prev, { role: 'assistant', content: `⚠️ ${msg}` }])
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const isBlocked = !user

  return (
    <div className="app">
      {/* ヘッダー */}
      <header className="header">
        <h1 className="title">✨ {t.title}</h1>
        <p className="subtitle">{t.subtitle}</p>

        <div className="header-controls">
          <div className="language-selector">
            <span className="globe">🌐</span>
            {(['en', 'ja', 'ko'] as Language[]).map(lang => (
              <button
                key={lang}
                className={language === lang ? 'active' : ''}
                onClick={() => setLanguage(lang)}
              >
                {lang === 'en' ? 'English' : lang === 'ja' ? '日本語' : '한국어'}
              </button>
            ))}
          </div>

          {user ? (
            <div className="user-info">
              {user.picture && <img src={user.picture} alt={user.name} className="user-avatar" />}
              {isUnlimited ? (
                <span className="prompt-badge prompt-badge--unlimited">{t.unlimited} ✨</span>
              ) : queriesRemaining !== null ? (
                <span className={`prompt-badge${queriesRemaining <= 3 ? ' prompt-badge--low' : ''}`}>
                  {t.queriesLeft(queriesRemaining)}
                </span>
              ) : null}
              {!isUnlimited && (
                <button className="coupon-btn" onClick={() => { setShowCouponModal(true); setCouponStatus('idle') }}>{t.couponBtn}</button>
              )}
              <button className="sign-out-btn" onClick={handleSignOut}>{t.signOut}</button>
            </div>
          ) : null}
        </div>
      </header>

      {/* チャットエリア */}
      <main className="chat-area">
        <div className="messages">
          {messages.map((msg, idx) => (
            <div key={idx} className={`message ${msg.role}`}>
              {msg.role === 'assistant' && <div className="avatar">🤖</div>}
              <div className="message-content">
                <div className="message-text">{msg.content}</div>
                {msg.sources && msg.sources.length > 0 && (
                  <div className="sources">
                    <div className="sources-title">{t.sources}:</div>
                    {msg.sources.map(source => (
                      <div key={source.index} className="source-item">
                        <span className="source-badge">[{source.index}]</span>
                        <span className="source-filename">{source.filename}</span>
                        <span className="source-lang">({source.language})</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          ))}

          {isLoading && (
            <div className="message assistant">
              <div className="avatar">🤖</div>
              <div className="message-content">
                <div className="message-text loading">{t.sending}</div>
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>
      </main>

      {/* 入力エリア */}
      <footer className="input-area">
        {!user ? (
          <div className="signin-area">
            <GoogleLogin
              onSuccess={handleLoginSuccess}
              onError={() => console.error('Google login failed')}
              text="signin_with"
              shape="rectangular"
              theme="outline"
              size="large"
            />
          </div>
        ) : (
          <>
            <textarea
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={t.placeholder}
              rows={1}
              disabled={isLoading || isBlocked}
            />
            <button
              onClick={handleSend}
              disabled={isLoading || !input.trim()}
            >
              {t.send}
            </button>
          </>
        )}
      </footer>

      {/* Daily limit modal */}
      {showLimitModal && (
        <div className="modal-overlay" onClick={() => setShowLimitModal(false)}>
          <div className="modal paywall-modal" onClick={e => e.stopPropagation()}>
            <div className="paywall-icon">🌙</div>
            <h2 className="paywall-title">{t.limitTitle}</h2>
            <p className="paywall-body">{t.limitBody}</p>
            <button className="limit-close-btn" onClick={() => { setShowLimitModal(false); setShowCouponModal(true); setCouponStatus('idle') }}>
              {t.couponBtn}
            </button>
            <button className="sign-out-btn" style={{marginTop: '0.5rem', width: '100%', textAlign: 'center'}} onClick={() => setShowLimitModal(false)}>
              {t.limitClose}
            </button>
          </div>
        </div>
      )}

      {/* Coupon modal */}
      {showCouponModal && user && (
        <div className="modal-overlay" onClick={() => setShowCouponModal(false)}>
          <div className="modal paywall-modal" onClick={e => e.stopPropagation()}>
            <div className="paywall-icon">🎟️</div>
            <h2 className="paywall-title">{t.couponTitle}</h2>
            {couponStatus === 'success' ? (
              <p className="paywall-body" style={{color: '#4ade80'}}>{t.couponSuccess}</p>
            ) : (
              <>
                <div className="paywall-token-row" style={{marginTop: '1rem'}}>
                  <input
                    type="text"
                    value={couponInput}
                    onChange={e => { setCouponInput(e.target.value.toUpperCase()); setCouponStatus('idle') }}
                    placeholder={t.couponPlaceholder}
                    onKeyDown={e => e.key === 'Enter' && handleRedeemCoupon()}
                    autoFocus
                  />
                  <button onClick={handleRedeemCoupon} disabled={couponStatus === 'loading' || !couponInput.trim()}>
                    {couponStatus === 'loading' ? t.couponApplying : t.couponApply}
                  </button>
                </div>
                {couponStatus === 'invalid' && <p className="paywall-error">{t.couponInvalid}</p>}
                {couponStatus === 'error' && <p className="paywall-error">{t.couponError}</p>}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export default App
