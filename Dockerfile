FROM alpine as stripper

RUN apk add binutils
RUN apk --no-cache add ca-certificates

COPY tibber_refiner /tibber_refiner
RUN strip /tibber_refiner

FROM scratch as run

COPY --from=stripper /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=stripper /tibber_refiner /tibber_refiner

CMD ["/tibber_refiner"]
