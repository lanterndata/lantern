FROM debian:bookworm

ARG VERSION=15.4
ARG PGVECTOR_VERSION=0.5.0
ARG UBSAN=

WORKDIR /lantern
# This requires the docker command be run in the lantern base director
COPY lantern_hnsw/scripts scripts

WORKDIR pg_build

RUN apt-get update && \
    apt-mark hold locales && \
    apt-get install -y \
    wget \
    curl \
    build-essential \
    make \
    cmake \
    pkg-config \
    flex \
    bison \
    libicu-dev \
    libssl-dev \
    clang-15 \
    llvm-15 \
    llvm-15-dev \
    llvm-15-tools \
    libstdc++6

RUN wget -q -O postgresql.tar.bz2 \
     https://ftp.postgresql.org/pub/source/v${VERSION}/postgresql-${VERSION}.tar.bz2 && \
     tar --extract --file postgresql.tar.bz2 --directory . --strip-components 1

RUN patch -F5 -p1 -d . < /lantern/scripts/sanitizers/postgres-asan-instrumentation.patch

RUN groupadd -r postgres --gid=999 && \
     useradd -r -g postgres --uid=999 postgres 

ENV LLVM_CONFIG "llvm-config-15"
ENV CC "clang-15"
ENV CXX "clang-15"
ENV CFLAGS "-g -fsanitize=address${UBSAN} -fno-omit-frame-pointer -Og -fno-inline-functions"
ENV CXXFLAGS "-g -fsanitize=address${UBSAN} -fno-omit-frame-pointer -Og -fno-inline-functions"
ENV LDFLAGS "-fsanitize=address${UBSAN}"

RUN ./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert \
    --with-openssl --without-readline --without-zlib --without-libxml && \
    make -j$(nproc) && \
    make -j$(nproc) -C src/test/isolation && \
    make install

ENV PATH="/usr/local/pgsql/bin:${PATH}"
ENV LD_LIBRARY_PATH=:/usr/local/pgsql/lib
ENV PGDATA=/var/lib/postgresql/data
RUN mkdir -p ${PGDATA} && \
    chown -R postgres:postgres ${PGDATA} && \
    chmod 777 ${PGDATA}

WORKDIR /lantern

RUN mkdir /lantern/sanitizer && \
    chown -R postgres:postgres /lantern && \
    chmod 777 /lantern/sanitizer

ENV ASAN_OPTIONS suppressions=/lantern/scripts/sanitizers/suppressions/suppr_asan.txt \
  detect_odr_violation=0 log_path=/lantern/sanitizer/ \
  log_exe_name=true print_suppressions=false exitcode=27 \
  detect_leaks=0 abort_on_error=1

ENV LSAN_OPTIONS suppressions=/lantern/scripts/sanitizers/suppressions/suppr_leak.txt \
  print_suppressions=0 log_path=/lantern/sanitizer/ \
  log_exe_name=true print_suppressions=false exitcode=27

ENV UBSAN_OPTIONS suppressions=/lantern/scripts/sanitizers/suppressions/suppr_ub.txt \
  print_stacktrace=1 halt_on_error=1 log_path=/lantern/sanitizer/ \
  log_exe_name=true print_suppressions=false exitcode=27

RUN wget --quiet -O pgvector.tar.gz https://github.com/pgvector/pgvector/archive/refs/tags/v${PGVECTOR_VERSION}.tar.gz && \
    tar xf pgvector.tar.gz && \
    cd pgvector-${PGVECTOR_VERSION} && \
    make && make install

COPY lantern_hnsw .

# lantern sources contain cpp code, so we need cpp compiler
ENV CXX "clang++-15"
RUN rm -rf build && \
    mkdir build && \
    cd build && \
    CXXFLAGS="-g -fsanitize=address -fno-omit-frame-pointer -Og -fno-inline-functions" cmake .. && \
    make install

USER postgres
RUN initdb -D ${PGDATA} && \
    echo "local   all             all                                     trust" >  ${PGDATA}/pg_hba.conf && \
    echo "host    all             all             127.0.0.1/32            trust" >>  ${PGDATA}/pg_hba.conf && \
    echo "host    all             all             ::1/128                 trust" >>  ${PGDATA}/pg_hba.conf

CMD ["postgres", "-D", "/var/lib/postgresql/data"]
