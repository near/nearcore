FROM ubuntu:24.04

ARG TZ
ENV TZ="$TZ"

# Install basic development tools and iptables/ipset
RUN apt-get update && apt-get install -y --no-install-recommends \
  less \
  tmux \
  git \
  procps \
  sudo \
  fzf \
  zsh \
  man-db \
  unzip \
  gnupg2 \
  gh \
  curl \
  wget \
  iptables \
  ipset \
  iproute2 \
  dnsutils \
  aggregate \
  jq \
  nano \
  vim \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG USERNAME=ubuntu
RUN id -u $USERNAME &>/dev/null || useradd -m -s /bin/bash $USERNAME
# Ensure default user has access to /usr/local/share
RUN mkdir -p /usr/local/share/ && \
  chown -R $USERNAME:$USERNAME /usr/local/share

# Allow passwordless sudo for the default user
RUN echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/$USERNAME \
  && chmod 0440 /etc/sudoers.d/$USERNAME

# Persist bash history.
RUN mkdir /commandhistory \
  && touch /commandhistory/.bash_history \
  && chown -R $USERNAME /commandhistory
ENV PROMPT_COMMAND="history -a"
ENV HISTFILE=/commandhistory/.bash_history

# Set `DEVCONTAINER` environment variable to help with orientation
ENV DEVCONTAINER=true

# Create workspace and config directories and set permissions
RUN mkdir -p /workspace /home/$USERNAME/.claude && \
  chown -R $USERNAME:$USERNAME /workspace /home/$USERNAME/.claude

WORKDIR /workspace
# Set up non-root user
USER $USERNAME

# Set the default shell to zsh rather than sh
ENV SHELL=/bin/zsh

# Set the default editor and visual
ENV EDITOR=vim
ENV VISUAL=vim

# Default powerline10k theme
ARG ZSH_IN_DOCKER_VERSION=1.2.0
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v${ZSH_IN_DOCKER_VERSION}/zsh-in-docker.sh)" -- \
  -p git \
  -p fzf \
  -a "source /usr/share/doc/fzf/examples/key-bindings.zsh" \
  -a "source /usr/share/doc/fzf/examples/completion.zsh" \
  -a "export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
  -x

# Install nearcore build deps (per NEAR docs)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
  binutils-dev \
  libcurl4-openssl-dev \
  zlib1g-dev \
  libdw-dev \
  libiberty-dev \
  cmake \
  gcc \
  g++ \
  python3 \
  docker.io \
  python3-pip \
  build-essential \
  libssl-dev \
  pkg-config \
  bc \
  sysstat \
  python3-venv \
  protobuf-compiler \
  clang \
  llvm \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Rust (required by nearcore build)
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=$PATH:$CARGO_HOME/bin

RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
RUN chown -R $USERNAME:$USERNAME $CARGO_HOME $RUSTUP_HOME

# Pytest venv (created here; packages installed by postCreateCommand)
ENV VIRTUAL_ENV=/opt/nearcore-venv
ENV PATH=$VIRTUAL_ENV/bin:$PATH
RUN mkdir -p $VIRTUAL_ENV && python3 -m venv $VIRTUAL_ENV && chown -R $USERNAME:$USERNAME $VIRTUAL_ENV

# Copy init-pyvenv script into the image
COPY init-pyvenv.sh /usr/local/bin/init-pyvenv.sh
RUN chmod 555 /usr/local/bin/init-pyvenv.sh

USER $USERNAME
# Install Claude
ENV HOME=/home/$USERNAME
RUN curl -fsSL https://claude.ai/install.sh | bash
ENV PATH=$HOME/.local/bin:$PATH