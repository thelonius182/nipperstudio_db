suppressWarnings(suppressPackageStartupMessages(library(DBI)))
suppressWarnings(suppressPackageStartupMessages(library(googlesheets4)))
suppressWarnings(suppressPackageStartupMessages(library(dplyr)))
suppressWarnings(suppressPackageStartupMessages(library(stringr)))
suppressWarnings(suppressPackageStartupMessages(library(stringi)))
suppressWarnings(suppressPackageStartupMessages(library(lubridate)))
suppressWarnings(suppressPackageStartupMessages(library(readr)))

ns_con <- dbConnect(odbc::odbc(), "wpdev_mariadb", timeout = 10, encoding = "CP850")

sqlstmt <- "
select * from wp_nipper_main_playlists
"
playlists <- dbGetQuery(conn = ns_con, statement = sqlstmt)

sqlstmt <- "
select * from wp_nipper_tracklists
"
tracks <- dbGetQuery(conn = ns_con, statement = sqlstmt)

sqlstmt <- "
select * from wp_nipper_blocks
"
blocks <- dbGetQuery(conn = ns_con, statement = sqlstmt)

sqlstmt <- "
truncate table wp_nipper_pgm_titles
"
sql_result <- dbExecute(conn = ns_con, statement = sqlstmt)

sqlstmt <- "
update wp_nipper_main_playlists set finished = 3 where id = 32
"

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
where po1.id = 692764 and (tx1.taxonomy = 'programma_maker' or tx1.taxonomy = 'programma_genre' and tx1.parent > 0)
"
db_post_details <- dbGetQuery(conn = ns_con, statement = sqlstmt)
post_details <- db_post_details %>% group_by(pgm_id, term_name, term_type) %>% 
  mutate(rank = row_number()) %>% ungroup() %>% 
  filter(rank == 1) %>% select(-rank)

pgm_editor <- post_details %>% filter(term_type == 'pgm_editor') %>% select(term_name)
sqlstmt <- paste0("select display_name, id as wp_users_id from wp_users where display_name = '", 
                  pgm_editor$term_name, "'")
db_users <- dbGetQuery(conn = ns_con, statement = sqlstmt)

post_details <- post_details %>% left_join(db_users, by = c("term_name" = "display_name"))

sql_result <- dbExecute(conn = ns_con, statement = sqlstmt)

dbAppendTable(conn = ns_con, "wp_nipper_pgm_titles", pgm_titles)

dbDisconnect(ns_con)

ns_week <- read_rds(file = "C:/cz_salsa/cz_exchange/nipperstudio_week.RDS")

ns_week.1 <- ns_week %>% 
  mutate(pgm_title_playlist = cz_slot_value %>% 
           stri_trans_general("latin-ascii") %>% 
           str_replace_all("[ ]", "_") %>% 
           str_replace_all("[-:()!&'\"]", "") %>% 
           str_to_lower())

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Nipper Next spreadsheet op GD openen, na aanmelden bij Google
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
gs4_auth(email = "cz.teamservice@gmail.com")

# url_pfx: "docs.google.com/spreadsheets/d/"
url_wp_gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"

gd_wp_gidsinfo_raw <- read_sheet(ss = url_wp_gidsinfo, sheet = "gids-info")
gd_wp_gidsinfo.1 <- gd_wp_gidsinfo_raw %>% 
  select(pgm_key = `key-modelrooster`, nipper_mogelijk) %>% 
  filter(nipper_mogelijk == "J")

pl_ts_format <- stamp("20230513_za17.", orders = "%Y%Om%d_%a%H", quiet = T)

ns_week.2 <- ns_week.1 %>% 
  inner_join(gd_wp_gidsinfo.1, by = c("cz_slot_value" = "pgm_key")) %>% 
  mutate(pl_name = paste0(pl_ts_format(date_time), size, "_", pgm_title_playlist))

sqlstmt <- "
select * from wp_nipper_pgm_titles
"
titles_db <- dbGetQuery(conn = ns_con, statement = sqlstmt)

titles_db_to_be_deleted <- titles_db %>% 
  anti_join(pgm_titles, by = c("pgm_title" = "pgm_title"))  

ids_to_be_deleted <- str_flatten(titles_db_to_be_deleted$id, collapse = ", ")

sqlstmt <- "
delete from wp_nipper_pgm_titles where id in (@IDS)
"
sqlstmt <- sqlstmt %>% str_replace("@IDS", ids_to_be_deleted)

sql_result <- dbExecute(conn = ns_con, statement = sqlstmt)

titles_db_to_be_added <- pgm_titles %>% anti_join(titles_db, by = c("pgm_title" = "pgm_title"))

dbAppendTable(conn = ns_con, "wp_nipper_pgm_titles", titles_db_to_be_added)

sqlstmt <- "
select * from salsa_stats_all_pgms
"
all_pgms_db <- dbGetQuery(conn = ns_con, statement = sqlstmt)


all_pgms_db.1 <- all_pgms_db %>% 
  filter(pgmStart > "20220101_00") %>% 
  mutate(start_ts = ymd_h(pgmStart), 
         stop_ts = ymd_h(pgmStop),
         playlist_len = str_pad(as.character(time_length(interval(start_ts, stop_ts), unit = "minutes")), 
                                width = 3,
                                side = "left",
                                pad = "0"),
         playlist_start = str_pad(as.character(hour(start_ts)),
                                  width = 2,
                                  side = "left",
                                  pad = "0"),
         dated_slot = paste0(pl_ts_format(start_ts), playlist_start, ".", playlist_len),
         playlist_slot = str_sub(playlist_day, 10),
         editor_cleaned = post_editor %>% 
           str_to_lower() %>% 
           stri_trans_general("latin-ascii") %>% 
           str_replace_all("[ ,]", "_") %>% 
           str_replace_all("[-.:()!&'\"]", ""))

editor_names <- all_pgms_db.1 %>% 
  # mutate(editor_cleaned = str_replace(editor_cleaned, "fovre", "fevre")) %>% 
  select(editor_cleaned) %>% 
  filter(!(editor_cleaned %in% c("aukelien_van_hoyema", "amsterdam_klezmer_band", 
                                 "cz", "redactie_concertzender_actueel", "menso_groeneveld_theo_van_deventer")) 
         & str_length(editor_cleaned) > 0
         & str_detect(editor_cleaned, "__", negate = T)) %>% 
  distinct() %>% arrange(editor_cleaned)

sqlstmt <- "
select po1.id, po1.post_date, tr1.term_taxonomy_id from wp_posts po1
   left join wp_term_relationships tr1 on tr1.object_id = po1.id
where post_type = 'programma' and post_date = '2023-06-08 14:00' and term_taxonomy_id = 5
"
result <- dbGetQuery(conn = ns_con, statement = sqlstmt)
