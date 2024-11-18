ARG VARIANT="3.11-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ARG USERNAME=vscode
ARG WORKSPACE_FOLDER="/workspaces/app"
ARG TMP_CONFIG_CONTAINER="/tmp/config_container"
WORKDIR $WORKSPACE_FOLDER

# -----------------------------------------------------------------------------
# Define en variables
# -----------------------------------------------------------------------------
ENV WORKSPACE_PATH=$WORKSPACE_FOLDER
ENV PYTHONPATH="$WORKSPACE_PATH/src";"$WORKSPACE_PATH/src/streamlit/snow_observer/modules"
# Prevent interactive popups when initializing debian.
ENV DEBIAN_FRONTEND=noninteractive
# Preparing vars to install the SnowSQL CLI for the vscode user
ENV SNOWSQL_DEST="/home/$USERNAME/bin"
ENV SNOWSQL_LOGIN_SHELL="/home/$USERNAME/.bashrc"
ENV SNOWSQL_BOOTSTRAP="1.2"
ENV SNOWSQL_VERSION="1.2.32"
# Add SnowSQL to PATH
ENV PATH $SNOWSQL_DEST:$PATH

# -----------------------------------------------------------------------------
# Install OS packages
# -----------------------------------------------------------------------------
RUN sudo apt-get update && apt-get -yq install vim dos2unix

# -----------------------------------------------------------------------------
# Create the necessary directories
# -----------------------------------------------------------------------------
# Create temp folder to configure the container
RUN mkdir $TMP_CONFIG_CONTAINER
# create folders for the user and set permissions
RUN mkdir -p /home/${USERNAME}/.snowsql \
    && chown -R ${USERNAME}:${USERNAME} /home/${USERNAME}/.snowsql \
    && chmod -R 700 /home/${USERNAME}/.snowsql

# -----------------------------------------------------------------------------
# Install python requirements
# -----------------------------------------------------------------------------
COPY /.devcontainer/requirements*.txt $TMP_CONFIG_CONTAINER/.
RUN pip install --upgrade pip
RUN pip install -r $TMP_CONFIG_CONTAINER/requirements.txt -r $TMP_CONFIG_CONTAINER/requirements-dev.txt

# -----------------------------------------------------------------------------
# Persist bash history between sessions
# -----------------------------------------------------------------------------
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && mkdir /commandhistory \
    && touch /commandhistory/.bash_history \
    && chown -R $USERNAME /commandhistory \
    && echo "$SNIPPET" >> "/home/$USERNAME/.bashrc"

# -----------------------------------------------------------------------------
# install the SnowSQL CLI
# -----------------------------------------------------------------------------
RUN wget -P $TMP_CONFIG_CONTAINER https://sfc-repo.azure.snowflakecomputing.com/snowsql/bootstrap/$SNOWSQL_BOOTSTRAP/linux_x86_64/snowsql-$SNOWSQL_VERSION-linux_x86_64.bash && \
    chmod a+x $TMP_CONFIG_CONTAINER/snowsql-$SNOWSQL_VERSION-linux_x86_64.bash && \
    SNOWSQL_DEST=$SNOWSQL_DEST SNOWSQL_LOGIN_SHELL=$SNOWSQL_LOGIN_SHELL bash -c "$TMP_CONFIG_CONTAINER/snowsql-$SNOWSQL_VERSION-linux_x86_64.bash" $USERNAME

# -----------------------------------------------------------------------------
# Clean up temp folder used to configure the container
# -----------------------------------------------------------------------------
RUN rm -rvf $TMP_CONFIG_CONTAINER
