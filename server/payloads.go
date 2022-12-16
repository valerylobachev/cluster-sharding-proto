package server

type Response struct {
	Key    string `json:"key"`
	Server string `json:"server"`
	Res    string `json:"res"`
	Err    string `json:"err,omitempty"`
}

type Request struct {
	Key string `json:"key"`
	Msg string `json:"msg"`
}
