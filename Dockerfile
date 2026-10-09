# open-geocode server image: the release binary on a slim base. Packs are mounted, not baked in:
#   docker run -v /srv/packs/au:/packs/pack -p 8080:8080 ghcr.io/flolep2607/open-geocode:latest
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked && strip target/release/open-geocode

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/open-geocode /usr/local/bin/open-geocode
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD curl -fs http://127.0.0.1:8080/readyz || exit 1
ENTRYPOINT ["open-geocode"]
CMD ["serve", "--pack", "/packs/pack", "--bind", "0.0.0.0:8080"]
