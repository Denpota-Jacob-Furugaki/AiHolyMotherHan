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
  display_name: string
  source_type: string
  author?: string
  // Speech
  speech_date?: string
  speech_title?: string
  speech_location?: string
  // Book
  book_name?: string
  book_name_en?: string
  book_name_ko?: string
  chapter?: string
  chapter_title?: string
  section?: string
  section_title?: string
  // Web
  website?: string
  url?: string
  // Common
  s3_key: string
  language: string
  original_language?: string
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
  email?: string
  picture: string
  idToken: string
}

interface AdminUser {
  email: string
  name: string
  picture: string
  first_seen: string
  last_active: string
  query_date: string
  query_count: number
  total_queries: number
  unlimited: boolean
}

interface AdminSummary {
  total_users: number
  active_today: number
  total_queries_today: number
  total_queries_all: number
  unlimited_users: number
  date: string
}

interface DashboardData {
  summary: AdminSummary
  users: AdminUser[]
}

const ADMIN_EMAILS = new Set(['denpotafurugaki@gmail.com'])

// ============================================================
// In-app browser detection
// ============================================================

function isInAppBrowser(): boolean {
  const ua = navigator.userAgent || navigator.vendor || ''
  // LINE, Facebook, Instagram, Twitter/X, WeChat, KakaoTalk, NAVER, etc.
  return /Line\//i.test(ua)
    || /FBAN|FBAV/i.test(ua)
    || /Instagram/i.test(ua)
    || /Twitter|X\.com/i.test(ua)
    || /MicroMessenger/i.test(ua)
    || /KAKAOTALK/i.test(ua)
    || /NAVER/i.test(ua)
    || /Snapchat/i.test(ua)
    || /Discord/i.test(ua)
    || (/wv\)/i.test(ua) && /Android/i.test(ua))
}

const IN_APP = isInAppBrowser()

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
  const [showAdmin, setShowAdmin] = useState(false)
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null)
  const [dashboardLoading, setDashboardLoading] = useState(false)

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const t = TEXTS[language]
  const isAdmin = user?.email ? ADMIN_EMAILS.has(user.email) : false

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
      const base64 = idToken.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')
      const binary = atob(base64)
      const bytes = Uint8Array.from(binary, c => c.charCodeAt(0))
      const payload = JSON.parse(new TextDecoder().decode(bytes))
      const googleUser: GoogleUser = {
        sub: payload.sub,
        name: payload.name || payload.email,
        email: payload.email || '',
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
      // Build conversation history (skip index-0 greeting, take last 6 turns)
      const history = messages
        .slice(1)
        .slice(-6)
        .map(m => ({ role: m.role, content: m.content }))

      const response = await fetch(`${API_ENDPOINT}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: userMessage,
          language,
          google_id_token: user.idToken,
          history,
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

  const fetchDashboard = async () => {
    if (!user || !isAdmin) return
    setDashboardLoading(true)
    try {
      const res = await fetch(`${API_ENDPOINT}/admin/dashboard`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ google_id_token: user.idToken }),
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data: DashboardData = await res.json()
      setDashboardData(data)
    } catch (e) {
      console.error('Dashboard fetch failed', e)
    } finally {
      setDashboardLoading(false)
    }
  }

  const openAdmin = () => {
    setShowAdmin(true)
    fetchDashboard()
  }

  const formatDate = (iso: string) => {
    if (!iso) return '—'
    try {
      const d = new Date(iso)
      return d.toLocaleDateString('ja-JP', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
    } catch { return iso }
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
              {isAdmin && <button className="coupon-btn" onClick={openAdmin}>📊 Admin</button>}
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
                        <div className="source-details">
                          <span className="source-display-name">{source.display_name || source.filename}</span>
                          <div className="source-meta">
                            {source.author && <span className="source-author">👤 {source.author}</span>}
                            {source.speech_date && <span className="source-date">📅 {source.speech_date}</span>}
                            {source.speech_location && <span className="source-location">📍 {source.speech_location}</span>}
                            {source.chapter && <span className="source-chapter">{source.chapter}{source.section_title ? ` — ${source.section_title}` : ''}</span>}
                            <span className="source-lang">{source.language === 'ja' ? '🇯🇵' : source.language === 'ko' ? '🇰🇷' : '🇺🇸'} {source.language}{source.original_language && source.original_language !== source.language ? ` (← ${source.original_language})` : ''}</span>
                          </div>
                        </div>
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
          IN_APP ? (
            <div className="inapp-warning">
              <div className="inapp-icon">🌐</div>
              <p className="inapp-title">
                {language === 'ja' ? 'ブラウザで開いてください' : language === 'ko' ? '브라우저에서 열어주세요' : 'Please open in your browser'}
              </p>
              <p className="inapp-body">
                {language === 'ja'
                  ? 'アプリ内ブラウザではGoogleログインが使えません。Safari / Chromeで開いてください。'
                  : language === 'ko'
                  ? '앱 내 브라우저에서는 Google 로그인이 불가합니다. Safari / Chrome에서 열어주세요.'
                  : 'Google Sign-In is blocked in in-app browsers. Please open this page in Safari or Chrome.'}
              </p>
              <div className="inapp-buttons">
                <button className="inapp-btn" onClick={() => {
                  navigator.clipboard?.writeText(window.location.href)
                    .then(() => alert(language === 'ja' ? 'URLをコピーしました！ブラウザに貼り付けてください。' : language === 'ko' ? 'URL이 복사되었습니다! 브라우저에 붙여넣으세요.' : 'URL copied! Paste it in your browser.'))
                    .catch(() => alert(window.location.href))
                }}>
                  {language === 'ja' ? '📋 URLをコピー' : language === 'ko' ? '📋 URL 복사' : '📋 Copy URL'}
                </button>
                <a className="inapp-btn inapp-btn-primary" href={`intent://${window.location.host}${window.location.pathname}#Intent;scheme=https;package=com.android.chrome;end`}
                   onClick={(e) => {
                     // For iOS, try opening in Safari via a trick
                     if (/iPhone|iPad/i.test(navigator.userAgent)) {
                       e.preventDefault()
                       window.location.href = window.location.href
                     }
                   }}>
                  {language === 'ja' ? '🚀 ブラウザで開く' : language === 'ko' ? '🚀 브라우저에서 열기' : '🚀 Open in Browser'}
                </a>
              </div>
            </div>
          ) : (
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
          )
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

      {/* Admin dashboard modal */}
      {showAdmin && isAdmin && (
        <div className="modal-overlay" onClick={() => setShowAdmin(false)}>
          <div className="modal admin-modal" onClick={e => e.stopPropagation()}>
            <div className="admin-header">
              <h2>📊 Admin Dashboard</h2>
              <button className="admin-close" onClick={() => setShowAdmin(false)}>✕</button>
            </div>
            {dashboardLoading ? (
              <div className="admin-loading">Loading...</div>
            ) : dashboardData ? (
              <>
                <div className="admin-summary">
                  <div className="admin-card">
                    <div className="admin-card-value">{dashboardData.summary.total_users}</div>
                    <div className="admin-card-label">Total Users</div>
                  </div>
                  <div className="admin-card">
                    <div className="admin-card-value">{dashboardData.summary.active_today}</div>
                    <div className="admin-card-label">Active Today</div>
                  </div>
                  <div className="admin-card">
                    <div className="admin-card-value">{dashboardData.summary.total_queries_today}</div>
                    <div className="admin-card-label">Queries Today</div>
                  </div>
                  <div className="admin-card">
                    <div className="admin-card-value">{dashboardData.summary.total_queries_all}</div>
                    <div className="admin-card-label">Total Queries</div>
                  </div>
                  <div className="admin-card">
                    <div className="admin-card-value">{dashboardData.summary.unlimited_users}</div>
                    <div className="admin-card-label">Unlimited Users</div>
                  </div>
                </div>
                <div className="admin-table-wrap">
                  <table className="admin-table">
                    <thead>
                      <tr>
                        <th></th>
                        <th>User</th>
                        <th>First Seen</th>
                        <th>Last Active</th>
                        <th>Today</th>
                        <th>Total</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dashboardData.users.map((u, i) => (
                        <tr key={i}>
                          <td>{u.picture ? <img src={u.picture} alt="" className="admin-avatar" /> : '👤'}</td>
                          <td>
                            <div className="admin-user-name">{u.name || '—'}</div>
                            <div className="admin-user-email">{u.email || '—'}</div>
                          </td>
                          <td>{formatDate(u.first_seen)}</td>
                          <td>{formatDate(u.last_active)}</td>
                          <td>{u.query_date === dashboardData.summary.date ? u.query_count : 0}</td>
                          <td>{u.total_queries}</td>
                          <td>{u.unlimited ? <span className="admin-badge-unlimited">Unlimited</span> : <span className="admin-badge-free">Free</span>}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="admin-footer">
                  <button className="coupon-btn" onClick={fetchDashboard} disabled={dashboardLoading}>🔄 Refresh</button>
                  <span className="admin-date">Date: {dashboardData.summary.date} (JST)</span>
                </div>
              </>
            ) : (
              <div className="admin-loading">Failed to load dashboard</div>
            )}
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
