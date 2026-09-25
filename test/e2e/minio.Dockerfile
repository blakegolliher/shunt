# The e2e MinIO, built from its public source. MinIO withdrew its public images (2026-09-25: quay.io
# and Docker Hub answer 401 to anonymous pulls), so CI builds the release the e2e backends pinned.
# MinIO is AGPL-3.0: it runs here as a tool, as Garage does, and no MinIO code is copied into shunt.
FROM docker.io/library/golang:1.24-alpine AS build
ARG MINIO_RELEASE=RELEASE.2025-07-23T15-54-02Z
ARG MINIO_VERSION=2025-07-23T15:54:02Z
RUN apk add --no-cache git
RUN git clone --depth 1 --branch "$MINIO_RELEASE" https://github.com/minio/minio /src
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN go build -trimpath -o /minio -ldflags "-s -w \
      -X github.com/minio/minio/cmd.Version=$MINIO_VERSION \
      -X github.com/minio/minio/cmd.ReleaseTag=$MINIO_RELEASE \
      -X github.com/minio/minio/cmd.CommitID=$(git rev-parse HEAD) \
      -X github.com/minio/minio/cmd.ShortCommitID=$(git rev-parse --short=12 HEAD)" .

FROM docker.io/library/alpine:3.20
COPY --from=build /minio /usr/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
