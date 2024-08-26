package commands

func (api *BaseAPI) SetL2RpcUrl(url string) {
	api.l2RpcUrl = url
}

func (api *BaseAPI) GetL2RpcUrl() string {
	if len(api.l2RpcUrl) == 0 {
		panic("L2RpcUrl is not set")
	}
	return api.l2RpcUrl
}

// For X Layer
func (api *BaseAPI) SetL2RpcLimit(limit int64) {
	api.l2RpcLimit = limit
}
