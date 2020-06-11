package mcast

// Represents an asynchronous operation that
// will have a response or error in the future.
// When a method is called it will block until a final
// value is received, this applies both to responses and errors.
type Future interface {
	// If the request contains a response and did not fail
	// the value will be available here
	Response() interface{}

	// Returns the error if the request failed.
	Error() error
}

// Used when the unity is shutdown.
type ShutdownFuture struct {
	unity *Unity
}

// ShutdownFuture implements Future.
func (s *ShutdownFuture) Response() interface{} {
	if s.unity == nil {
		return nil
	}
	s.unity.state.group.Wait()
	s.Error()
	return nil
}

// ShutdownFuture implements Future.
func (s *ShutdownFuture) Error() error {
	if s.unity == nil {
		return nil
	}

	s.unity.state.group.Wait()
	for _, peer := range s.unity.state.Nodes {
		if err := peer.Trans.Close(); err != nil {
			return err
		}
	}

	return nil
}
