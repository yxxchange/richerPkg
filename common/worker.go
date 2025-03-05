package common

type Processor[T, V any] interface {
	Process(param T) (V, error)
}

type Transporter[T any] interface {
	Transport(param T) error
}
