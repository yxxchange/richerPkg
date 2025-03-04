package easy_watcher

import (
	"batch_scheduler/common"
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
)

type Watcher[T any] struct {
	req    *http.Request
	cli    *http.Client
	logger common.Logger
	trans  common.Transporter[T]
}

func NewWatcher[T any]() *Watcher[T] {
	return &Watcher[T]{
		logger: &common.DefaultLogger{},
	}
}

func (w *Watcher[T]) RegisterTransporter(trans common.Transporter[T]) {
	w.trans = trans
}

func (w *Watcher[T]) RegisterWatchModule(req *http.Request) {
	w.req = req
	w.req.Header.Set("Connection", "keep-alive")
}

func (w *Watcher[T]) RegisterClient(cli *http.Client) {
	w.cli = cli
}

func (w *Watcher[T]) RegisterLogger(logger common.Logger) {

}

func (w *Watcher[T]) Watch() error {
	if w.cli == nil {
		w.cli = &http.Client{}
	}

	resp, err := w.cli.Do(w.req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if err = canWatch(resp); err != nil {
		return err
	}

	reader := bufio.NewReader(resp.Body)
	decoder := json.NewDecoder(reader)

	for {
		var data T
		if err = decoder.Decode(&data); err != nil {
			if err.Error() == "EOF" {
				w.logger.Infof("流式传输结束")
				break
			}
			w.logger.Infof("解析错误: %v", err)
			continue
		}
		err = w.trans.Transport(data)
		if err != nil {
			w.logger.Infof("传输错误: %v", err)
		}
	}

	return nil
}

func canWatch(resp *http.Response) error {
	chunkedFlag := false
	for _, v := range resp.TransferEncoding {
		if v == "chunked" {
			chunkedFlag = true
		}
	}
	if !chunkedFlag {
		return fmt.Errorf("server does not support chunked encoding")
	}
	return nil
}
