#!/bin/bash

. "$TOOLS_DIR/color.sh"

echo -e "\n${BLUE}#############################################################${ENDCOLOR}"
echo -e "${BLUE}#####                                                   #####${ENDCOLOR}"
echo -e "${BLUE}#####              Configure Snow CLI connexion         #####${ENDCOLOR}"
echo -e "${BLUE}#####                                                   #####${ENDCOLOR}"
echo -e "${BLUE}#############################################################${ENDCOLOR}"

echo -e "\n${GREEN}> Configure Snow CLI connexion.${ENDCOLOR}\n"

# export git config to bashrc
snow connection add --connection-name default --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER

echo -e "\nDone\n"
