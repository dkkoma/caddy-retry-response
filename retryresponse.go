// Package retryresponse provides a Caddy HTTP middleware module that re-executes
// a request based on the response status of the downstream handler (php_server etc.).
//
// It reproduces the behavior of nginx's `fastcgi_next_upstream http_429 non_idempotent;`
// at the Caddy layer. A typical use case: the application converts a retryable
// condition (e.g. Google Cloud Spanner's AbortedException, after sleeping for the
// retry delay) into an HTTP 429 response; when this module sees the 429 it replays
// the same request instead of returning the response to the client. The module does
// not wait between attempts — any backoff is expected to happen in the application.
//
// To replay the request, the body is buffered in memory (up to memory_buffer) and
// spilled to a temp file beyond that. Bodies larger than max_body are not buffered
// and not retried (the request runs exactly once, leaving enforcement to the site's
// request_body limits and the like).
package retryresponse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultAttempts     = 10       // matches a typical nginx upstream server count (= total tries)
	defaultMemoryBuffer = 50 << 20 // 50MiB
	defaultMaxBody      = 1 << 30  // 1GiB

	// non-standard status from nginx meaning "client closed the request" (for access logs)
	statusClientClosedRequest = 499
)

func init() {
	caddy.RegisterModule(&Handler{})
	httpcaddyfile.RegisterHandlerDirective("retry_response", parseCaddyfile)
	// ディレクティブの並び順をグローバル順序リストの route 直前に登録する。
	// Caddyfile 内の書き順とは無関係に、アダプタはこのリスト内の位置でソートし、
	// 生成された handle 配列は前にある要素ほど外側になるよう畳み込まれる
	// (caddyhttp.RouteList.Compile)。php_server / php / php_fastcgi / reverse_proxy /
	// file_server はいずれも route 以降に並ぶため、本ハンドラは必ずそれらの外側で
	// next を呼ぶ立場になり、応答ステータスを見てから再実行できる。
	// アンカーに使えるのは Caddy 標準ディレクティブだけ(プラグイン同士の読み込み
	// 順は保証されないため php_server を直接指せない)なので route を使っている
	httpcaddyfile.RegisterDirectiveOrder("retry_response", httpcaddyfile.Before, "route")
}

// Handler is a middleware that replays the buffered request body and re-executes
// the downstream handler when the response status is retryable.
type Handler struct {
	// HTTP statuses to retry on. Defaults to [429]
	Statuses []int `json:"statuses,omitempty"`

	// Total number of tries including the first one. Defaults to 10.
	// If the final attempt still yields a retryable status, that response is
	// returned to the client as-is
	Attempts int `json:"attempts,omitempty"`

	// Maximum request body size kept in memory (bytes). Anything beyond spills
	// to a temp file. Defaults to 50MiB
	MemoryBuffer int64 `json:"memory_buffer,omitempty"`

	// Maximum request body size to buffer (bytes). Larger bodies are neither
	// buffered nor retried; the request runs exactly once. Defaults to 1GiB
	MaxBody int64 `json:"max_body,omitempty"`

	// Directory for temp files. Defaults to the OS temp dir
	TempDir string `json:"temp_dir,omitempty"`

	// Statuses を検索用に変換したセット(map[int]struct{} は「キーの存在だけが
	// 情報で、値は無意味」を表す Go の定番イディオム)。retryable 判定はレスポンス
	// のたびに走るホットパスなので O(1) で引けるようにしておく。Provision で
	// 構築した後は読み取り専用なので、ロックなしで全リクエストから並行に共有できる。
	// 非公開フィールドは JSON マーシャル対象外なので、設定としては露出しない
	statusSet map[int]struct{}
	logger    *zap.Logger
	retries   *prometheus.CounterVec
}

var retryResponseMetrics = struct {
	once    sync.Once
	retries *prometheus.CounterVec
}{}

// CaddyModule implements caddy.Module.
func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.retry_response",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision implements caddy.Provisioner.
func (h *Handler) Provision(ctx caddy.Context) error {
	return h.provision(ctx.Logger(), ctx.GetMetricsRegistry())
}

// provision applies defaults and builds internal state (split out so tests can call it directly).
//
// デフォルト値の適用を UnmarshalCaddyfile ではなくここで行うのは意図的。
// Caddyfile はアダプタで JSON へ変換される糖衣にすぎず、JSON で直接設定されると
// パーサは一切通らない。どの設定経路でも必ず通るこの場所に置かないと、
// 経路によってデフォルトの有無が分裂する
func (h *Handler) provision(logger *zap.Logger, registry *prometheus.Registry) error {
	h.logger = logger
	retries, err := initRetryResponseMetrics(registry)
	if err != nil {
		return err
	}
	h.retries = retries
	if len(h.Statuses) == 0 {
		h.Statuses = []int{http.StatusTooManyRequests}
	}
	if h.Attempts == 0 {
		h.Attempts = defaultAttempts
	}
	if h.MemoryBuffer == 0 {
		h.MemoryBuffer = defaultMemoryBuffer
	}
	if h.MaxBody == 0 {
		h.MaxBody = defaultMaxBody
	}
	if h.MemoryBuffer > h.MaxBody {
		h.MemoryBuffer = h.MaxBody
	}
	if h.TempDir == "" {
		h.TempDir = os.TempDir()
	} else if err := os.MkdirAll(h.TempDir, 0o700); err != nil {
		return fmt.Errorf("creating temp_dir: %v", err)
	}
	h.statusSet = make(map[int]struct{}, len(h.Statuses))
	for _, s := range h.Statuses {
		h.statusSet[s] = struct{}{}
	}
	return nil
}

func initRetryResponseMetrics(registry *prometheus.Registry) (*prometheus.CounterVec, error) {
	if registry == nil {
		return nil, nil
	}

	retryResponseMetrics.once.Do(func() {
		retryResponseMetrics.retries = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "caddy",
			Subsystem: "retry_response",
			Name:      "retries_total",
			Help:      "Counter of retry attempts triggered by retryable response statuses.",
		}, []string{"status"})
	})

	err := registry.Register(retryResponseMetrics.retries)
	if err == nil {
		return retryResponseMetrics.retries, nil
	}

	var alreadyRegistered prometheus.AlreadyRegisteredError
	if errors.As(err, &alreadyRegistered) {
		retries, ok := alreadyRegistered.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, fmt.Errorf("retry_response retries metric already registered with unexpected type %T", alreadyRegistered.ExistingCollector)
		}
		return retries, nil
	}

	return nil, fmt.Errorf("registering retry_response retries metric: %w", err)
}

// Validate implements caddy.Validator.
//
// 設定ロード時(起動・リロード)に Provision 成功の直後に一度だけ呼ばれ、
// リクエスト処理中には呼ばれない。Provision の後なのでデフォルト適用済みの値を
// 検査すればよい(attempts 未指定 = 0 はここに来る前に 10 へ埋められている)。
// エラーを返すと設定ロード全体が拒否され、リロード時は旧設定が動き続ける(fail fast)
func (h *Handler) Validate() error {
	if h.Attempts < 1 {
		return fmt.Errorf("attempts must be >= 1, got %d", h.Attempts)
	}
	for _, s := range h.Statuses {
		// 1xx can never be a final status, so it cannot be retried on
		if s < 200 || s > 599 {
			return fmt.Errorf("invalid retry status: %d", s)
		}
	}
	if h.MemoryBuffer < 0 {
		return fmt.Errorf("memory_buffer must be >= 0, got %d", h.MemoryBuffer)
	}
	if h.MaxBody < 1 {
		return fmt.Errorf("max_body must be >= 1, got %d", h.MaxBody)
	}
	return nil
}

// ServeHTTP implements caddyhttp.MiddlewareHandler.
//
// リトライの正体は「next(自分より内側のチェーン全体)を最大 attempts 回呼び直す」こと。
// レスポンスは next の戻り値ではなく ResponseWriter への書き込み(副作用)として
// 発生するため、本物の w をそのまま渡すと retryable ステータスも書かれた瞬間に
// クライアントへ流れて取り消せない。そこで最終試行以外は w を attemptWriter に
// 差し替え、下流が WriteHeader を呼んだまさにその瞬間に「素通しするか、握りつぶして
// リトライするか」を決める
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	body, err := h.bufferRequestBody(r)
	if err != nil {
		return err
	}
	defer body.cleanup()

	if !body.replayable {
		// max_body 超過で再送不能なので、リトライなしの 1 回実行に切り替える。
		// サイト側の request_body 制限や PHP 側の post_max_size は通常どおり働く
		return next.ServeHTTP(w, r)
	}

	for attempt := 1; ; attempt++ {
		req := replayRequest(r, body)

		if attempt == h.Attempts {
			// 最終試行はもうリトライしないので、握りつぶす仕掛け自体が不要。
			// 素の w を渡して直接ストリームさせる(retryable ステータスでもそのまま返る)
			return next.ServeHTTP(w, req)
		}

		aw := newAttemptWriter(w, h.statusSet)
		if err := next.ServeHTTP(aw, req); err != nil {
			// Handler errors are not subject to status-based retry.
			// Leave them to Caddy's error handling
			return err
		}

		if aw.hijacked || aw.committed || !aw.wroteHeader {
			// リトライ不能・不要なケースを消去法で列挙している:
			//   hijacked     … WebSocket 等で接続ごと移譲済み。HTTP 応答の枠組みが終了
			//   committed    … 非 retryable ステータスを本物の w へ送信済みで取り消せない
			//   !wroteHeader … 何も書かれなかった。判定すべきステータスが存在せず、
			//                  Caddy の既定処理(空応答)に委ねる
			// この if を抜ける組み合わせは wroteHeader && !committed だけ、すなわち
			// 「retryable ステータスを観測して応答を破棄した」場合のみ下のリトライへ進む
			return nil
		}

		// Retryable status: discard the response and re-execute
		if err := r.Context().Err(); err != nil {
			// クライアントが既に切断しているならリトライは無意味。
			// nginx 慣習の 499 でアクセスログに残して打ち切る
			h.logger.Debug("client disconnected during retry",
				zap.Int("attempt", attempt),
				zap.Int("status", aw.status))
			return caddyhttp.Error(statusClientClosedRequest, err)
		}
		h.recordRetry(aw.status)
		h.logger.Debug("retrying request",
			zap.String("uri", r.RequestURI),
			zap.Int("attempt", attempt),
			zap.Int("status", aw.status))
	}
}

func (h *Handler) recordRetry(status int) {
	if h.retries == nil {
		return
	}
	h.retries.WithLabelValues(strconv.Itoa(status)).Inc()
}

// bufferedBody is a request body buffered in a replayable form.
//
// r.Body はソケットからの一方通行ストリームで巻き戻せないため、リトライで同じ
// ボディを再送するには、最初の試行の前に全体を読み取って何度でも読み直せる形で
// 保持しておく必要がある。置き場所は 3 段階:
//
//	≤ memory_buffer … メモリ (mem)
//	≤ max_body      … 一時ファイル (file)
//	> max_body      … バッファ断念 (replayable=false、リトライなしの 1 回実行)
type bufferedBody struct {
	replayable bool
	mem        []byte
	file       *os.File
	fileName   string // 開いたまま unlink できない環境 (Windows 等) での後始末用。空なら unlink 済み
	size       int64
}

// newReader returns a fresh reader from the start of the body, independent per attempt.
// SectionReader は同じ fd 上に独立した読み位置を持てるため、試行ごとのリーダーが
// オフセットを共有せず互いに干渉しない
func (b *bufferedBody) newReader() io.ReadCloser {
	if b.file != nil {
		return io.NopCloser(io.NewSectionReader(b.file, 0, b.size))
	}
	return io.NopCloser(bytes.NewReader(b.mem))
}

func (b *bufferedBody) cleanup() {
	if b.file != nil {
		_ = b.file.Close()
		b.file = nil
	}
	if b.fileName != "" {
		_ = os.Remove(b.fileName)
		b.fileName = ""
	}
}

// bufferRequestBody buffers the request body in memory / a temp file.
// If the body exceeds max_body it sets replayable = false and swaps r.Body for a
// reader that stitches the already-read part to the unread rest (for a single,
// retry-free execution)
func (h *Handler) bufferRequestBody(r *http.Request) (*bufferedBody, error) {
	b := &bufferedBody{replayable: true}

	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return b, nil
	}
	if r.ContentLength > h.MaxBody {
		// max_body 超過が宣言されているなら 1 バイトも読まずにリトライ断念を確定。
		// r.Body は無傷のまま下流ハンドラへ渡る
		b.replayable = false
		return b, nil
	}

	if r.ContentLength > h.MemoryBuffer {
		// メモリに収まらないことが読む前から確定しているので、RAM 段階を飛ばして
		// ソケット → ファイルへ直接ストリームする(先頭部分の二度書きと bytes.Buffer
		// の倍々成長による一時的なメモリ消費を避ける)。chunked は ContentLength == -1
		// なのでこの分岐には入らず、下のメモリ優先パスに落ちる
		f, err := h.createSpillFile(b)
		if err != nil {
			return nil, err
		}
		// 宣言された長さは信用せず、実際に流れたバイト数で max_body 超過を判定する
		// (上限 +1 の番兵読み。仕組みは下のメモリパスのコメントを参照)
		n, err := io.Copy(f, io.LimitReader(r.Body, h.MaxBody+1))
		if err != nil {
			b.cleanup()
			return nil, requestBodyReadError(r, err)
		}
		b.size = n
		if b.size > h.MaxBody {
			markBodyPassthrough(r, b)
		}
		return b, nil
	}

	var buf bytes.Buffer
	growBufferForContentLength(&buf, r.ContentLength)
	// 上限 +1 バイトの「番兵読み」: ちょうど MemoryBuffer で読むのを止めると、
	// ボディがそこで尽きたのか続きがあるのか区別できない。1 バイト余分に読もうと
	// 試みることで、n <= MemoryBuffer なら上限の手前でストリームが尽きた(全部
	// 読めた)、n == MemoryBuffer+1 なら続きがある(ファイルへ spill)と判定できる。
	// LimitReader は上限到達を黙って EOF にするだけなので、err は本物の読み取り
	// 失敗(クライアント切断など)に限られる
	n, err := io.Copy(&buf, io.LimitReader(r.Body, h.MemoryBuffer+1))
	if err != nil {
		return nil, requestBodyReadError(r, err)
	}
	if n <= h.MemoryBuffer {
		b.mem = buf.Bytes()
		b.size = n
		return b, nil
	}

	// メモリに収まらなかったので、RAM に読み済みの先頭部分ごと一時ファイルへ退避する
	f, err := h.createSpillFile(b)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		b.cleanup()
		return nil, caddyhttp.Error(http.StatusInternalServerError, err)
	}
	// 残りをファイルへコピー。ここでも番兵読み: バッファしてよい残量は MaxBody-n
	// なので +1 まで読み、合計が MaxBody を超えたかどうかで超過を判定する
	rest, err := io.Copy(f, io.LimitReader(r.Body, h.MaxBody-n+1))
	if err != nil {
		b.cleanup()
		return nil, requestBodyReadError(r, err)
	}
	b.size = n + rest

	if b.size > h.MaxBody {
		markBodyPassthrough(r, b)
	}
	return b, nil
}

// markBodyPassthrough は max_body 超過が判明したときのフォールバック。
//
// この時点で先頭 max_body+1 バイト前後は既にソケットから読み出してしまっており
// (手元のバッファ/ファイルの中にある)、残りはまだソケットの中。リトライは諦める
// としても 1 回目の実行には完全なボディが必要なので、退避済みの先頭と未読の続きを
// MultiReader で直列に繋ぎ、下流からは先頭から読める 1 本のストリームに見えるよう
// r.Body を差し替える。なおここに来るのは実質 chunked のボディだけ
// (長さが宣言されていれば bufferRequestBody 冒頭の門前払いで弾かれている)
func markBodyPassthrough(r *http.Request, b *bufferedBody) {
	b.replayable = false
	r.Body = &passthroughBody{
		Reader: io.MultiReader(io.NewSectionReader(b.file, 0, b.size), r.Body),
		closer: r.Body,
	}
}

// createSpillFile はメモリからあふれた分(または最初から収まらないと分かっている
// ボディ)の受け皿となる一時ファイルを作る
func (h *Handler) createSpillFile(b *bufferedBody) (*os.File, error) {
	f, err := os.CreateTemp(h.TempDir, "retry-response-*")
	if err != nil {
		return nil, caddyhttp.Error(http.StatusInternalServerError, err)
	}
	// 作成直後に unlink する(Unix の定石)。開いている fd からは読み書きし続け
	// られるが名前は消えるため、プロセスがどう異常終了してもゴミファイルが残らない。
	// 開いたまま削除できない環境ではファイル名を覚えておき、cleanup が Close 後に削除する
	b.fileName = f.Name()
	if err := os.Remove(b.fileName); err == nil {
		b.fileName = ""
	}
	b.file = f
	return f, nil
}

// growBufferForContentLength は宣言された長さをヒントにバッファを事前確保し、
// bytes.Buffer の倍々成長(確保 → コピーの繰り返しと、一時的な約 2 倍のメモリ
// 消費)を避ける
func growBufferForContentLength(buf *bytes.Buffer, contentLength int64) {
	// chunked (-1) 等はヒントなし
	if contentLength <= 0 {
		return
	}
	// int が 32bit の環境での桁あふれ保護(Grow に負数を渡すと panic する)
	if int64(int(contentLength)) != contentLength {
		return
	}
	buf.Grow(int(contentLength))
}

// requestBodyReadError はボディ読み取り失敗を HTTP エラーへ変換する。
// コンテキスト取り消し = クライアント切断は、リクエスト不正 (400) ではなく
// クライアント都合の切断として nginx 慣習の 499 で記録する。
// err が既に HandlerError の場合(例: request_body の MaxBytesReader 由来の 413)、
// caddyhttp.Error は設定済みステータスを保持するため、ここの 400 に化けることはない
func requestBodyReadError(r *http.Request, err error) error {
	if ctxErr := r.Context().Err(); ctxErr != nil {
		return caddyhttp.Error(statusClientClosedRequest, ctxErr)
	}
	return caddyhttp.Error(http.StatusBadRequest, err)
}

// passthroughBody is a one-shot reader stitching the buffered part to the unread body
type passthroughBody struct {
	io.Reader
	closer io.Closer
}

func (p *passthroughBody) Close() error { return p.closer.Close() }

// replayRequest builds a per-attempt request carrying the buffered body.
// ヘッダは Clone で深いコピーになるため、下流が加えた変更は試行間で漏れない。
// 一方 Caddy の vars はリクエストコンテキスト内の共有マップなので、
// 試行をまたいで意図的に共有される
func replayRequest(r *http.Request, body *bufferedBody) *http.Request {
	req := r.Clone(r.Context())
	req.Body = body.newReader()
	req.ContentLength = body.size
	req.TransferEncoding = nil
	if body.size > 0 {
		// chunked で受けたボディも全量バッファ済みで長さが確定しているので、
		// Content-Length を付け直し Transfer-Encoding を落として普通のリクエストに直す
		req.Header.Set("Content-Length", strconv.FormatInt(body.size, 10))
		req.Header.Del("Transfer-Encoding")
	}
	return req
}

// attemptWriter is the ResponseWriter used for every attempt except the last.
//
// レスポンスは ResponseWriter への書き込み(副作用)で発生し、本物へ書いた瞬間に
// 取り消せなくなる。そこで下流には本物の代わりにこれを渡し、WriteHeader が呼ばれた
// 瞬間にステータスを見て「素通しか、破棄か」を決める:
//   - retryable ステータス … 応答全体(ヘッダ/ボディ)を破棄する
//   - それ以外            … 本物へそのままストリームする
//
// 隔離の不変条件は「破棄されうる試行は本物の writer に痕跡を残さない」。
// ハンドラに見せるヘッダは構築時に取ったクローン (header) で、本物の rw.Header()
// には応答を確定させる syncHeader まで一切書かない。これにより破棄された試行の
// ヘッダ/ボディはクライアントへ届かず、次の試行は本物からクリーンなスナップ
// ショットを取り直せる(唯一の例外である 1xx の即時送信は、送信後にヘッダを
// 復元することでこの不変条件を守っている。WriteHeader を参照)。
//
// Unwrap は意図的に実装しない。実装すると http.ResponseController がこのラッパーを
// 素通りして本物へ到達し、破棄すべき応答をクライアントへ flush できてしまう。
// 代償として Flush / Hijack 以外の ResponseController 操作は非最終試行では
// ErrNotSupported になる(README の Out of scope 参照)
type attemptWriter struct {
	rw        http.ResponseWriter // 本物。ここに書いたら取り消せない
	header    http.Header         // この試行専用のクローン。ハンドラの Header() はこちらを見る
	retryable map[int]struct{}

	// wroteHeader と committed は別々の状態機械を追っている:
	//   wroteHeader … ハンドラ側: 最終ステータスを宣言したか(WriteHeader は 1 回きり)
	//   committed   … クライアント側: 本物の rw へ実際に流し始めたか
	// 破棄された試行は wroteHeader=true かつ committed=false になり、
	// この組み合わせがリトライループへの「破棄済み」の合図になる
	status      int // 観測した最終ステータス(デバッグログ用)
	wroteHeader bool
	committed   bool
	hijacked    bool
}

func newAttemptWriter(w http.ResponseWriter, retryable map[int]struct{}) *attemptWriter {
	return &attemptWriter{
		rw: w,
		// サーバや外側のミドルウェアがプリセットしたヘッダ (Server: Caddy 等) を
		// 試行から見え、かつ削除もできるようにするためのスナップショット
		header:    w.Header().Clone(),
		retryable: retryable,
	}
}

// Header implements http.ResponseWriter.
func (a *attemptWriter) Header() http.Header { return a.header }

// WriteHeader implements http.ResponseWriter.
func (a *attemptWriter) WriteHeader(code int) {
	if a.hijacked || a.wroteHeader {
		return
	}
	if code >= 100 && code <= 199 {
		// 1xx は最終ステータスではないため転送する。1xx はその時点のヘッダ付きで
		// 即時送信するしかないので、実 writer のヘッダマップを一時的にこの試行の
		// 内容へ差し替えて送信し、送信後に元へ戻す。戻さずに残すと、この試行が
		// 破棄されたとき次の試行のスナップショット (newAttemptWriter) にヘッダが
		// 混入し、最終レスポンスへ漏れる
		saved := a.rw.Header().Clone()
		a.syncHeader()
		a.rw.WriteHeader(code)
		replaceHeader(a.rw.Header(), saved)
		return
	}
	// 破棄する場合でも wroteHeader は立てる。「宣言された」と「送信した」は別で、
	// これを立てないと (1) 2 回目の WriteHeader を無視する本物の contract を再現
	// できず、(2) 続く Write が暗黙の WriteHeader(200) を発動して committed になり、
	// 破棄したはずのボディが 200 で流出し、(3) リトライループが「破棄済み」
	// (wroteHeader && !committed) を検出できなくなる
	a.wroteHeader = true
	a.status = code
	if _, retry := a.retryable[code]; retry {
		// retryable: 本物には何も書かず、この試行の応答を丸ごと破棄する
		return
	}
	a.syncHeader()
	a.rw.WriteHeader(code)
	a.committed = true
}

// syncHeader replaces the real ResponseWriter's header map with this attempt's headers
func (a *attemptWriter) syncHeader() {
	replaceHeader(a.rw.Header(), a.header)
}

// replaceHeader は dst の中身を src と過不足なく一致させる。dst には呼び出し元の
// ResponseWriter がプリセットしたヘッダ (Server 等) が入っており、試行がそれを
// 削除した場合はその削除も反映する必要があるため、単なる上書きではなく空にして
// から詰め直す。dst のマップは ResponseWriter が参照を保持しているので作り直さず
// in-place で入れ替える。dst と src は常に別インスタンスである前提
func replaceHeader(dst, src http.Header) {
	clear(dst)
	maps.Copy(dst, src)
}

// Write implements http.ResponseWriter.
func (a *attemptWriter) Write(p []byte) (int, error) {
	if a.hijacked {
		return 0, http.ErrHijacked
	}
	if !a.wroteHeader {
		// 本物と同じく、宣言前の Write は暗黙の 200 を意味する
		a.WriteHeader(http.StatusOK)
	}
	if a.committed {
		return a.rw.Write(p)
	}
	// 破棄が決まった試行のボディは読み捨てる(成功を装って長さを返す)
	return len(p), nil
}

// Flush implements http.Flusher.
func (a *attemptWriter) Flush() {
	// 本物の Flusher と同じく、ステータス未宣言のまま Flush されたら 200 で確定させる
	if !a.wroteHeader && !a.hijacked {
		a.WriteHeader(http.StatusOK)
	}
	if a.committed {
		_ = http.NewResponseController(a.rw).Flush()
	}
}

// Hijack implements http.Hijacker. Hijacked connections (WebSocket etc.) are never retried
func (a *attemptWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(a.rw).Hijack()
	if err == nil {
		a.hijacked = true
	}
	return conn, rw, err
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler. Syntax:
//
//	retry_response {
//	    statuses      <code...>
//	    attempts      <n>
//	    memory_buffer <size>
//	    max_body      <size>
//	    temp_dir      <path>
//	}
//
// ここは構文 → 構造体の変換だけを行い、デフォルト適用や検証はしない。
// Caddyfile はアダプタで JSON に変換される糖衣で、JSON 直接設定ではこの関数を
// 通らないため、意味論は設定経路に関係なく必ず通る provision / Validate に置く
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name
	if d.NextArg() {
		return d.ArgErr()
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "statuses":
			args := d.RemainingArgs()
			if len(args) == 0 {
				return d.ArgErr()
			}
			for _, arg := range args {
				status, err := strconv.Atoi(arg)
				if err != nil {
					return d.Errf("parsing status %q: %v", arg, err)
				}
				h.Statuses = append(h.Statuses, status)
			}
		case "attempts":
			if !d.NextArg() {
				return d.ArgErr()
			}
			attempts, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("parsing attempts %q: %v", d.Val(), err)
			}
			h.Attempts = attempts
		case "memory_buffer":
			size, err := parseSizeArg(d)
			if err != nil {
				return err
			}
			h.MemoryBuffer = size
		case "max_body":
			size, err := parseSizeArg(d)
			if err != nil {
				return err
			}
			h.MaxBody = size
		case "temp_dir":
			if !d.NextArg() {
				return d.ArgErr()
			}
			h.TempDir = d.Val()
		default:
			return d.Errf("unrecognized subdirective %q", d.Val())
		}
	}
	return nil
}

func parseSizeArg(d *caddyfile.Dispenser) (int64, error) {
	subdirective := d.Val()
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	size, err := humanize.ParseBytes(d.Val())
	if err != nil {
		return 0, d.Errf("parsing %s %q: %v", subdirective, d.Val(), err)
	}
	if size > uint64(1<<62) {
		return 0, d.Errf("%s too large: %s", subdirective, d.Val())
	}
	return int64(size), nil
}

// parseCaddyfile は RegisterHandlerDirective が要求するシグネチャを満たすための糊。
// パース本体を caddyfile.Unmarshaler のメソッドに置いて委譲するのが Caddy の定型
func parseCaddyfile(helper httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var h Handler
	if err := h.UnmarshalCaddyfile(helper.Dispenser); err != nil {
		return nil, err
	}
	return &h, nil
}

// Interface guards
//
// Go のインターフェースは暗黙的に満たされるため、シグネチャが変わって実装から
// 外れてもそれ自体はコンパイルエラーにならない。しかも Caddy や
// http.ResponseController はこれらを実行時の型アサーションで発見するので、
// 壊れると「Provision が呼ばれずデフォルトが効かない」「Flush が伝播しない」
// といった静かな実行時バグになる。nil ポインタの代入という形でコンパイル時に
// 実装を検査しておく(実行時コストはゼロ)
var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
	_ http.Flusher                = (*attemptWriter)(nil)
	_ http.Hijacker               = (*attemptWriter)(nil)
)
