package kucoin

import (
	"crypto/tls"
	"testing"

	"github.com/gorilla/websocket"
)

func TestNewWebSocketDialerDoesNotMutateDefault(t *testing.T) {
	original := websocket.DefaultDialer
	defaultTLSConfig := &tls.Config{ServerName: "default.example"}
	websocket.DefaultDialer = &websocket.Dialer{
		ReadBufferSize:  4096,
		TLSClientConfig: defaultTLSConfig,
	}
	defer func() {
		websocket.DefaultDialer = original
	}()

	dialer := newWebSocketDialer(true)
	if dialer == websocket.DefaultDialer {
		t.Fatal("expected a private WebSocket dialer")
	}
	if dialer.ReadBufferSize != webSocketReadBufferSize {
		t.Fatalf("unexpected read buffer size: got %d, want %d", dialer.ReadBufferSize, webSocketReadBufferSize)
	}
	if !dialer.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("expected TLS verification to be disabled on the private dialer")
	}
	if websocket.DefaultDialer.ReadBufferSize != 4096 {
		t.Fatalf("default read buffer size changed: got %d, want 4096", websocket.DefaultDialer.ReadBufferSize)
	}
	if websocket.DefaultDialer.TLSClientConfig != defaultTLSConfig {
		t.Fatal("default TLS config was replaced")
	}
	if websocket.DefaultDialer.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("default TLS verification setting changed")
	}

	dialer.TLSClientConfig.ServerName = "private.example"
	if websocket.DefaultDialer.TLSClientConfig.ServerName != "default.example" {
		t.Fatal("private dialer shares its TLS config with the default dialer")
	}
}

func TestApiService_WebSocketPublicToken(t *testing.T) {
	s := NewApiServiceFromEnv()
	rsp, err := s.WebSocketPublicToken()
	if err != nil {
		t.Fatal(err)
	}
	pt := &WebSocketTokenModel{}
	if err := rsp.ReadData(pt); err != nil {
		t.Fatal(err)
	}
	t.Log(pt.Token)
	switch {
	case pt.Token == "":
		t.Error("Empty key 'token'")
	case len(pt.Servers) == 0:
		t.Fatal("Empty key 'instanceServers'")
	}
	for _, s := range pt.Servers {
		t.Log(ToJsonString(s))
		switch {
		case s.Endpoint == "":
			t.Error("Empty key 'endpoint'")
		case s.Protocol == "":
			t.Fatal("Empty key 'protocol'")
		}
	}
}

func TestApiService_WebSocketPrivateToken(t *testing.T) {
	s := NewApiServiceFromEnv()
	rsp, err := s.WebSocketPrivateToken()
	if err != nil {
		t.Fatal(err)
	}
	pt := &WebSocketTokenModel{}
	if err := rsp.ReadData(pt); err != nil {
		t.Fatal(err)
	}
	t.Log(pt.Token)
	switch {
	case pt.Token == "":
		t.Error("Empty key 'token'")
	case len(pt.Servers) == 0:
		t.Fatal("Empty key 'instanceServers'")
	}
	for _, s := range pt.Servers {
		t.Log(ToJsonString(s))
		switch {
		case s.Endpoint == "":
			t.Error("Empty key 'endpoint'")
		case s.Protocol == "":
			t.Fatal("Empty key 'protocol'")
		}
	}
}

func TestWebSocketClient_Connect(t *testing.T) {
	s := NewApiServiceFromEnv()

	rsp, err := s.WebSocketPublicToken()
	if err != nil {
		t.Fatal(err)
	}

	tk := &WebSocketTokenModel{}
	if err := rsp.ReadData(tk); err != nil {
		t.Fatal(err)
	}

	c := s.NewWebSocketClient(tk)

	_, _, err = c.Connect()
	if err != nil {
		t.Fatal(err)
	}
}
func TestWebSocketClient_Subscribe(t *testing.T) {
	t.SkipNow()

	s := NewApiServiceFromEnv()

	rsp, err := s.WebSocketPublicToken()
	if err != nil {
		t.Fatal(err)
	}

	tk := &WebSocketTokenModel{}
	if err := rsp.ReadData(tk); err != nil {
		t.Fatal(err)
	}

	c := s.NewWebSocketClient(tk)

	mc, ec, err := c.Connect()
	if err != nil {
		t.Fatal(err)
	}

	ch1 := NewSubscribeMessage("/market/ticker:KCS-BTC", false)
	ch2 := NewSubscribeMessage("/market/ticker:ETH-BTC", false)
	uch := NewUnsubscribeMessage("/market/ticker:ETH-BTC", false)

	if err := c.Subscribe(ch1, ch2); err != nil {
		t.Fatal(err)
	}

	var i = 0
	for {
		select {
		case err := <-ec:
			c.Stop() // Stop subscribing the WebSocket feed
			t.Fatal(err)
		case msg := <-mc:
			t.Log(ToJsonString(msg))
			i++
			if i == 5 {
				t.Log("Unsubscribe ETH-BTC")
				if err = c.Unsubscribe(uch); err != nil {
					t.Fatal(err)
				}
			}
			if i == 10 {
				t.Log("Subscribe ETH-BTC")
				if err = c.Subscribe(ch2); err != nil {
					t.Fatal(err)
				}
			}
			if i == 15 {
				t.Log("Exit subscribing")
				c.Stop() // Stop subscribing the WebSocket feed
				return
			}
		}
	}
}
