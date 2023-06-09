pacman::p_load(DBI, googlesheets4, dplyr, stringr, stringi, lubridate, readr, fs, futile.logger)

f1 <- flog.appender(appender.file("c:/cz_salsa/Logs/nipperstudio_build_week.log"), name = "nsbw_log")
flog.info("
= = = = = NipperStudio Nieuwe Week, Start = = = = =", name = "nsbw_log")
rds_home <- "C:/cz_salsa/cz_exchange/"
source("src/shared_functions.R", encoding = "UTF-8") 

# nieuwe playlists voor WP ----
# + nieuwe gidsweek ophalen ----
ns_week <- read_rds(paste0(rds_home, "nipperstudio_week.RDS"))
log_txt <- paste0("deze week: ", min(ns_week$date_time), " - ", max(ns_week$date_time))
flog.info(log_txt, name = "nsbw_log")

# + pgm-titel normalseren ----
ns_week.1 <- ns_week %>% 
  mutate(pgm_title_playlist = cz_slot_value %>% 
           stri_trans_general("latin-ascii") %>% 
           str_replace_all("[ ]", "_") %>% 
           str_replace_all("[-:()!&'\"]", "") %>% 
           str_to_lower())

# + gidsinfo 1 ----
# weet welke titels in aanmerking komen
gs4_auth(email = "cz.teamservice@gmail.com")
url_wp_gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"
gd_wp_gidsinfo_raw <- read_sheet(ss = url_wp_gidsinfo, sheet = "gids-info")
gd_wp_gidsinfo.1 <- gd_wp_gidsinfo_raw %>% 
  select(pgm_key = `key-modelrooster`, nipper_mogelijk) %>% 
  # alleen deze!
  filter(nipper_mogelijk != "N")

# + gidsinfo 2 ----
# ns slugs
gd_wp_gidsinfo_slugs_raw <- read_sheet(ss = url_wp_gidsinfo, sheet = "nipperstudio_slugs")
titel_slugs <- gd_wp_gidsinfo_slugs_raw %>% select(starts_with("titel")) %>% filter(!is.na(titel_id))
redacteur_slugs <- gd_wp_gidsinfo_slugs_raw %>% select(starts_with("redacteur")) %>% filter(!is.na(redacteur_id))

# + alleen geldige overhouden ----
pl_ts_format <- stamp("20230513_za17.", orders = "%Y%Om%d_%a%H", quiet = T)
pl_ymd_format <- stamp("2023-05-13", orders = "%Y%Om%d", quiet = T)
ns_week.2 <- ns_week.1 %>% 
  inner_join(gd_wp_gidsinfo.1, by = c("cz_slot_value" = "pgm_key")) %>% 
  # naam normaliseren
  mutate(pl_name = paste0(pl_ts_format(date_time), size, "_", pgm_title_playlist),
         pl_ts_ymd = pl_ymd_format(date_time),
         pl_ts_start = hour(date_time),
         pl_ts_stop = hour(date_time + dminutes(as.numeric(size))),
         pl_ts_stop = if_else(pl_ts_stop == 0, 24, as.double(pl_ts_stop))) %>% 
  select(pl_ts = date_time,
         pl_ts_ymd,
         pl_ts_start,
         pl_ts_stop,
         pl_title = cz_slot_value,
         pl_size = size,
         pl_name,
         nipper_mogelijk)

# WP-items ophalen ----
# + connect to DB ----
# ns_con <- dbConnect(odbc::odbc(), "wpdev_mariadb", timeout = 10, encoding = "CP850")
ns_con <- get_ns_conn("DEV")

stopifnot("WP-database is niet beschikbaar, zie C:/cz_salsa/Logs/nipperstudio_build_week.log" = typeof(ns_con) == "S4")
flog.info("Verbonden!", name = "nsbw_log")

# # + connect to DB
# ns_con <- dbConnect(odbc::odbc(), "wpdev_mariadb", timeout = 10, encoding = "CP850")

# + posts ----
ts_list <- paste0("('", ns_week.2$pl_ts %>% str_flatten(collapse = "', '"), "')")
sqlstmt <- str_replace("
select po1.post_date, po1.id from wp_posts po1
   left join wp_term_relationships tr1 on tr1.object_id = po1.id
where post_type = 'programma' and term_taxonomy_id = 5
  and post_date in @TSLIST
", "@TSLIST", ts_list)
post_ids <- dbGetQuery(conn = ns_con, statement = sqlstmt)
no_posts <- "Geen gepubliceerde posts gevonden voor deze week >> geen playlists toegevoegd aan NipperStudio."

if (nrow(post_ids) == 0) {
  flog.info(no_posts, name = "nsbw_log")
  dbDisconnect(ns_con)
}

# stopifnot accepteert geen variable voor de mededeling
stopifnot("Geen gepubliceerde posts gevonden voor deze week >> geen playlists toegevoegd aan NipperStudio." = nrow(post_ids) > 0)

wp_playlists <- ns_week.2 %>% 
  inner_join(post_ids, by = c("pl_ts" = "post_date")) %>% 
  select(post_id = id, playlist_name = pl_name, starts_with("pl_ts_"), nipper_mogelijk)

# + titels en redacteuren ----
poids <- paste0("('", post_ids$id %>% str_flatten(collapse = "', '"), "')")
sqlstmt <- "
select po1.id as pgm_id,
       te1.name as term_name,
       case when tx1.taxonomy = 'programma_genre' then 'pgm_title'
            when tx1.taxonomy = 'programma_maker' then 'pgm_editor'
            else 'not_needed'
	   end as term_type,
       te1.term_id
from wp_posts po1 
   left join wp_term_relationships tr1 on tr1.object_id = po1.id
   left join wp_terms te1 on te1.term_id = tr1.term_taxonomy_id
   left join wp_term_taxonomy tx1 on tx1.term_taxonomy_id= tr1.term_taxonomy_id
where (tx1.taxonomy = 'programma_maker' 
       or tx1.taxonomy = 'programma_genre' and tx1.parent > 0)
      and po1.id in @POIDS
"
sqlstmt <- str_replace(sqlstmt, "@POIDS", poids)
db_post_details <- dbGetQuery(conn = ns_con, statement = sqlstmt)
post_details <- db_post_details %>% group_by(pgm_id, term_type) %>% 
  mutate(rank = row_number()) %>% ungroup() %>% 
  filter(rank == 1) %>% select(post_id = pgm_id, everything(), -rank)

# + user_id's ----
pgm_editor <- post_details %>% filter(term_type == 'pgm_editor') %>% select(term_name)
editor_list <- paste0("('", pgm_editor$term_name %>% str_flatten(collapse = "', '"), "')")
sqlstmt <- "select display_name, id as user_id from wp_users where display_name in @EDLST"
sqlstmt <- str_replace(sqlstmt, "@EDLST", editor_list)
db_users <- dbGetQuery(conn = ns_con, statement = sqlstmt)
wp_user_ids <- post_details %>% 
  filter(term_type == "pgm_editor") %>% 
  left_join(db_users, by = c("term_name" = "display_name")) %>% 
  select(post_id, user_id)

# + wp_pgm_ids ----
wp_pgm_ids <- post_details %>% 
  filter(term_type == "pgm_title") %>% 
  select(post_id, program_id = term_id)

# playlist: compile & persist ----
# + maak de huidige ----
nipper_main_playlists_cur <- wp_pgm_ids %>% 
  inner_join(wp_user_ids) %>% 
  inner_join(wp_playlists) %>% 
  mutate(datetime_created = now(tzone = "Europe/Amsterdam"),
         finished = 0L,
         deleted = 0L,
         block_transition = if_else(nipper_mogelijk == "BUM", "LOB", nipper_mogelijk)) %>% 
  select(post_id,
         datetime_created,
         program_id,
         program_date = pl_ts_ymd,
         time_start = pl_ts_start,
         time_end = pl_ts_stop,
         user_id,
         finished,
         playlist_name,
         block_transition,
         deleted) 

# + valideer user ----
nipper_main_playlists_err <- nipper_main_playlists_cur %>% filter(is.na(user_id))

if (nrow(nipper_main_playlists_err) > 0) {
  pl_in_err <- str_flatten(nipper_main_playlists_err$playlist_name, collapse = ", ")
  flog.info(paste0("Samensteller van deze uitzending staat niet in de WP-tabel 'wp_users':\n",
                   pl_in_err, "\nNipperen niet mogelijk."), name = "nsbw_log")
  nipper_main_playlists_cur <- nipper_main_playlists_cur %>% filter(!is.na(user_id))
}

# + lees archief ----
nipper_main_playlists_his <- read_rds(paste0(rds_home, "nipper_main_playlists.RDS"))

# + nieuwe overhouden ----
nipper_main_playlists_new <- nipper_main_playlists_cur %>% 
  anti_join(nipper_main_playlists_his, by = c("program_date" = "program_date",
                                              "time_start" = "time_start"))

if (nrow(nipper_main_playlists_new) > 0) {
  # add new to his
  nipper_main_playlists_his <- nipper_main_playlists_his %>% 
    bind_rows(nipper_main_playlists_new)
  write_rds(nipper_main_playlists_his, paste0(rds_home, "nipper_main_playlists.RDS"))
  
  # add to database ----
  sql_result <- dbAppendTable(conn = ns_con, "wp_nipper_main_playlists", 
                              nipper_main_playlists_new %>% select(-post_id))
}

dbDisconnect(ns_con)

# bumper folders ----
# bufo_cur <- dir_ls("//uitzendmac-2/Data/Nipper/studiomontage/bumpers/", 
#                    type = "directory",
#                    recurse = F) %>% as_tibble()
# 
# pl_titles <- nipper_main_playlists_new %>% 
#   select(titel_id = program_id) %>% distinct() %>% 
#   inner_join(titel_slugs, by = c(as.integer()))
# 
# write_delim(bufo_cur, "c:/cz_salsa/bufo.tsv", delim = "\t")

flog.info("= = = = = NipperStudio Nieuwe Week, Stop = = = = =", name = "nsbw_log")
flog.info(" ", name = "nsbw_log")
