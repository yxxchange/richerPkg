package easy_watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// 模拟 Transporter
type mockTransporter[T any] struct {
	dataChan chan T
}

func newMockTransporter[T any]() *mockTransporter[T] {
	return &mockTransporter[T]{
		dataChan: make(chan T, 100),
	}
}

func (m *mockTransporter[T]) Transport(data T) error {
	m.dataChan <- data
	return nil
}

func TestWatcher(t *testing.T) {
	// 创建测试服务器
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 设置分块传输头
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// 持续发送数据
		for i := 0; i < 5; i++ {
			data := map[string]interface{}{
				"id":   i,
				"time": time.Now().Unix(),
			}
			jsonData, _ := json.Marshal(data)
			fmt.Fprintf(w, "%s", jsonData) // 分块格式
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
		}
	}))
	defer ts.Close()

	// 创建 Watcher 实例
	watcher := NewWatcher[map[string]interface{}]()

	// 创建请求
	req, err := http.NewRequest("GET", ts.URL, nil)
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}

	// 配置 Watcher
	transporter := newMockTransporter[map[string]interface{}]()
	watcher.RegisterTransporter(transporter)
	watcher.RegisterWatchModule(req)
	watcher.RegisterClient(&http.Client{
		Timeout: time.Second * 30,
	})

	// 启动监听（使用context控制超时）
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		if err := watcher.Watch(); err != nil {
			t.Errorf("Watch 执行失败: %v", err)
		}
	}()

	// 验证接收的数据
	var receivedCount int
	for {
		select {
		case data := <-transporter.dataChan:
			if _, ok := data["id"]; !ok {
				t.Errorf("接收数据格式错误: %v", data)
			}
			receivedCount++
			t.Logf("收到数据: %+v", data)
		case <-ctx.Done():
			if receivedCount < 3 {
				t.Errorf("接收数据不足，期望至少3条，实际收到%d条", receivedCount)
			}
			return
		}
	}
}

// 实现简单日志记录器
type testLogger struct{}

func (l *testLogger) Infof(format string, args ...interface{}) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}

func (l *testLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}
