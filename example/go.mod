module example

go 1.13

replace github.com/gladkikhartem/async => ../

require (
	github.com/gladkikhartem/async v0.3.0
	github.com/gorilla/mux v1.8.0
	golang.org/x/net v0.0.0-20201031054903-ff519b6c9102
	google.golang.org/grpc v1.33.1
)
