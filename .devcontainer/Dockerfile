FROM python:3.14-bookworm

# This mimics the behavior of the standard dev container images
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME -s /bin/bash \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Install Docker CLI and ping
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bullseye stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && apt-get install -y --no-install-recommends iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Add /home/vscode/.local/bin to PATH
RUN echo '### Add local bin to PATH' >> /home/vscode/.bashrc
RUN echo 'export PATH=$PATH:/home/vscode/.local/bin' >> /home/vscode/.bashrc

# python packages for development and testing, pinning versions
COPY constraints.txt /tmp/constraints.txt

# Switch to vscode user
USER vscode

# Install Python dependencies for development, deployment, and testing
# This is done here just to make the devcontainer start faster
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-warn-script-location -r /tmp/constraints.txt

# Install act
# (no need to be root but whatever)
RUN cd ~/.local && curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

# Install node through nvm and add PYTHONPATH to allow
# cdk to find modules imported from test subdir
RUN curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash \
    && echo "export PYTHONPATH=/workspaces/cbers-2-stac" >> ~/.bashrc
ENV NVM_DIR="/home/vscode/.nvm"
RUN . ~/.nvm/nvm.sh \
    && nvm install 24.0.0
RUN . ~/.nvm/nvm.sh \
    && npm install -g aws-cdk@latest
    