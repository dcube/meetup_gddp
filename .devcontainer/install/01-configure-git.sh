#!/bin/bash

. "$TOOLS_DIR/color.sh"

echo -e "\n${BLUE}#############################################################${ENDCOLOR}"
echo -e "${BLUE}#####                                                   #####${ENDCOLOR}"
echo -e "${BLUE}#####              Configure Git                        #####${ENDCOLOR}"
echo -e "${BLUE}#####                                                   #####${ENDCOLOR}"
echo -e "${BLUE}#############################################################${ENDCOLOR}"

echo -e "\n${GREEN}> Configure Git.${ENDCOLOR}\n"

# export git config to bashrc
cat <<EOF >> ~/.bashrc
# Set git configurations
git config --global --add safe.directory "$WORKSPACE_PATH"
git config --global core.eol lf
git config --global core.autocrlf false
git config --global pull.rebase true
git config --global user.email "$def load_table(session: Session, tbl: Dict[str, str | bool]) -> None:
    """ load table """
    try:
        raw_table = RawTable(
            session=session,
            name=str(tbl["table_name"])
            )

        raw_table.load_from_csv(
            location=str(tbl.get("stage_path")),
            file_format=str(tbl.get("file_format")),
            mode=WriteMode(str(tbl.get("mode")).lower()),
            force=bool(tbl.get("force"))
            )

        print(f"load table {str(tbl['table_name'])} succeeded")

    except Exception as err:
        print(f"load table {str(tbl['table_name'])} failed with error {err}")"
git config --global user.name "$GIT_USERNAME"
EOF

echo -e "\nDone\n"
