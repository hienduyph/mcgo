package mcgo

type Client struct{}

func (c *Client) Get(key string) (*Item, error) {
	// get the node for this key
	// send the request
	// wait for response
	// parse and return
	panic("impl")
}
