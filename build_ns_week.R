suppressWarnings(suppressPackageStartupMessages(library(DBI)))
suppressWarnings(suppressPackageStartupMessages(library(googlesheets4)))
suppressWarnings(suppressPackageStartupMessages(library(dplyr)))
suppressWarnings(suppressPackageStartupMessages(library(stringr)))
suppressWarnings(suppressPackageStartupMessages(library(stringi)))
suppressWarnings(suppressPackageStartupMessages(library(lubridate)))
suppressWarnings(suppressPackageStartupMessages(library(readr)))

# nieuwe playlists voor WP ----
# + nieuwe gidsweek ophalen ----
ns_week <- read_rds("C:/cz_salsa/cz_exchange/nipperstudio_week.RDS")

# + pgm-titel normalseren ----
ns_week.1 <- ns_week %>% 
  mutate(pgm_title_playlist = cz_slot_value %>% 
           stri_trans_general("latin-ascii") %>% 
           str_replace_all("[ ]", "_") %>% 
           str_replace_all("[-:()!&'\"]", "") %>% 
           str_to_lower())

# + gidsinfo ----
# weet welke titels in aanmerking komen
gs4_auth(email = "cz.teamservice@gmail.com")
url_wp_gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"
gd_wp_gidsinfo_raw <- read_sheet(ss = url_wp_gidsinfo, sheet = "gids-info")
gd_wp_gidsinfo.1 <- gd_wp_gidsinfo_raw %>% 
  select(pgm_key = `key-modelrooster`, nipper_mogelijk) %>% 
  filter(nipper_mogelijk == "J")

# + alleen geldige overhouden ----
pl_ts_format <- stamp("20230513_za17.", orders = "%Y%Om%d_%a%H", quiet = T)
ns_week.2 <- ns_week.1 %>% 
  inner_join(gd_wp_gidsinfo.1, by = c("cz_slot_value" = "pgm_key")) %>% 
  # naam normaliseren
  mutate(pl_name = paste0(pl_ts_format(date_time), size, "_", pgm_title_playlist)) %>% 
  select(pl_ts = date_time,
         pl_title = cz_slot_value,
         pl_size = size,
         pl_name)

# WP-items ophalen ----
ns_con <- dbConnect(odbc::odbc(), "wpdev_mariadb", timeout = 10, encoding = "CP850")

# + posts ----
ts_list <- paste0("('", ns_week.2$pl_ts %>% str_flatten(collapse = "', '"), "')")
sqlstmt <- str_replace("
select po1.post_date, po1.id from wp_posts po1
   left join wp_term_relationships tr1 on tr1.object_id = po1.id
where post_type = 'programma' and term_taxonomy_id = 5
  and post_date in @TSLIST
", "@TSLIST", ts_list)
post_ids <- dbGetQuery(conn = ns_con, statement = sqlstmt)

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
  filter(rank == 1) %>% select(-rank)

# + user_id's ----
pgm_editor <- post_details %>% filter(term_type == 'pgm_editor') %>% select(term_name)
editor_list <- paste0("('", pgm_editor$term_name %>% str_flatten(collapse = "', '"), "')")
sqlstmt <- "select display_name, id as wp_users_id from wp_users where display_name in @EDLST"
sqlstmt <- str_replace(sqlstmt, "@EDLST", editor_list)
db_users <- dbGetQuery(conn = ns_con, statement = sqlstmt)
