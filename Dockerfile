from golang:1.16 as build
workdir /wd
copy *.go go.mod go.sum ./
run go build -tags netgo -ldflags '-w -extldflags "-static"' -o /wd/app.run .


from alpine:latest
workdir /wd
copy --from=build /wd/app.run /wd/app.run

CMD ["/wd/app.run"]