--Updates in existing Process/parameters to follow on the MDS standard
Update META_RAW.PROCESS Set PROCESS_NAME=replace(PROCESS_NAME,'JP_DCL_MDS_Refresh','JP_DCL_MDS_Refresh_SDL_MDS')  where  USECASE_ID='249';
