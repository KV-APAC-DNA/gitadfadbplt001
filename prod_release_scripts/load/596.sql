UPDATE  META_RAW.PROCESS SET process_name=replace(process_name,'SDL_MDS_SDL_MDS','SDL_MDS')  WHERE PROCESS_ID IN
(
'1644',
'1645',
'1646'
);
