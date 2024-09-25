------------------------------------------------------------------------------
-- manage git repositories
------------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE GIT REPOSITORY IF NOT EXISTS &{data_domain}.UTILS.GIT_REPO
  API_INTEGRATION = &{data_domain}_GIT
  ORIGIN = '&{git_repo_uri}';

-- fetch the git repo to update it
ALTER GIT REPOSITORY &{data_domain}.UTILS.GIT_REPO FETCH;
