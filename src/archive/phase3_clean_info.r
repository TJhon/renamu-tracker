
librarian::shelf(
  stringi,
  stringr, fs, readr, dplyr, purrr, dotenv)


load_dot_env()
INPUT_ROOT <- Sys.getenv("INPUT_ROOT")

MAIN_ROOT <- file.path(INPUT_ROOT, "llm_table_parser", "df_parser")


file_metadata <- function(full_path) {
  name <- path_file(full_path)
  
  match <- str_match(name, "(\\d{4})(?:_(\\d+))?_page_(\\d+)\\.csv")
  
  if (!is.na(match[1,1])) {
    year <- as.integer(match[1,2])
    modulo <- match[1,3]  # puede ser NA
    page <- as.integer(match[1,4])
    
    return(list(year = year, modulo = modulo, page = page))
  }
  
  return(NULL)
}

csvs <- dir_ls(MAIN_ROOT, recurse = TRUE, glob = "*.csv")

dfs <- map(csvs, function(c) {
  tryCatch({
    
    if (file_info(c)$size == 0) {
      return(NULL)
    }
    
    df <- read_csv(c, show_col_types = FALSE)
    
    if (nrow(df) == 0) {
      return(NULL)
    }
    
    meta <- file_metadata(c)
    
    if (is.null(meta)) {
      return(NULL)
    }
    
    df <- df %>%
      mutate(
        year = meta$year,
        modulo = meta$modulo,
        page = meta$page
      )
    df <- df |> mutate_all(as.character) |> 
      mutate(row_id = row_number())
    
    return(df)
    
  }, error = function(e) {
    return(NULL)
  })
})



df_r <- bind_rows(dfs) |> 
  relocate(year, modulo, page, h2, n_q, cuadro, desc_q, col, desc_col, values, value_pos) |> 
  arrange(year, modulo, page, row_id)

df_r1 <- df_r |> 
  mutate(
    across(where(is.character), ~ tidyr::replace_na(.x, ""))
  ) |> 
  mutate(
    id = row_number()
  )


LIMIT_COL = 16

df_refom <- df_r1 |> 
  # select(page, id, year, h2, col, n_q, contains('desc'), c=cuadro) |> 
  mutate_all(
    \(x) str_trim(x)
  ) |> 
  mutate(
    col_cl = col,
    n_q_cl = n_q,
    desc_q_cl = desc_q,
    desc_col_cl = desc_col,
  ) |> 
  mutate(
    n_q_cl = ifelse(str_detect(n_q, "^\\d+$"), "", n_q_cl),
    # col_cl = case_when(
    #   str_length(col_cl) > str_length(n_q) & n_q_cl != "" ~ n_q,
    #   T ~ col_cl
    # ),
    col_cl = case_when(
      str_length(col_cl) > LIMIT_COL ~ "",
      T ~ col_cl
    ),
    col_cl = case_when(
      col_cl == "" & str_length(n_q_cl) <= LIMIT_COL ~ n_q,
      T ~ col_cl
    ),
    col_cl = case_when(
      str_detect(str_to_lower(col_cl), 'cuadro') ~ "",
      T ~ col_cl
      ),
    n_q_cl = case_when(
      col_cl == n_q & str_length(n_q_cl) <= LIMIT_COL ~ "",
      T ~ n_q_cl
    ),
    n_col_cl = str_length(col_cl)
  ) |> 
  # filter(col_cl != "") |>
  mutate(
    desc_col_cl = case_when(
      str_to_lower(desc_col_cl) == str_to_lower(col_cl) ~ "",
      T ~ desc_col_cl
    ),
    desc_col_cl = case_when(
      desc_col_cl == "" ~ desc_q_cl,
      T ~ desc_col_cl
    ),
    desc_q_cl = case_when(
      desc_col_cl == desc_q_cl ~ "",
      T ~ desc_q_cl
    )
  ) |> 
  mutate(
    id = as.integer(id),
    year = as.integer(year),
    c = cuadro, 
    cuadro = paste(
      ifelse(str_detect(str_to_lower(col), "^cuadro"), col, ""),
      ifelse(str_detect(str_to_lower(n_q), "^cuadro"), n_q, ""),
      ifelse(str_detect(str_to_lower(desc_q), "^cuadro"), desc_q, ""),
      ifelse(str_detect(str_to_lower(desc_col), "^cuadro"), desc_col, ""),
      ifelse(str_detect(str_to_lower(h2), "cuadro"), h2, ""),
      ifelse(str_detect(str_to_lower(col), "^c\\d+"), col, ""),
      ifelse(str_detect(str_to_lower(n_q), "^c\\d+"), n_q, ""),
      ifelse(str_detect(str_to_lower(desc_q), "^c\\d+"), desc_q, ""),
      ifelse(str_detect(str_to_lower(desc_col), "^c\\d+"), desc_col, ""),
      ifelse(str_detect(str_to_lower(h2), "^c\\d+"), h2, ""),
      ifelse(str_detect(col, "^\\d+$") & as.numeric(col) < 200, col, ""),
      ifelse(str_detect(n_q, "^\\d+$") & as.numeric(n_q) < 200, n_q, ""),
      ifelse(str_detect(desc_q, "^\\d+$") & as.numeric(desc_q) < 200, desc_q, ""),
      ifelse(str_detect(desc_col, "^\\d+$") & as.numeric(desc_col) < 200, desc_col, ""),
      ifelse(str_detect(h2, "^\\d+$") & as.numeric(h2) < 200, h2, ""),
      sep = "|"
    ), 
    cuadro = ifelse(year >= 2021, "", cuadro),
    n_c = str_length(cuadro),
  ) |> 
  relocate(c, cuadro) |> 
  # select(!c(col, h2, n_q, desc_q, desc_col)) |>
  mutate(col_cl = str_to_lower(col_cl)) |> 
  # filter(year < 2007) |> 
  arrange(id)


# aqui se tiene col_cl como aparece en la tabla y se espera que este dentro de los cuadros 
# dentro de los archivos de renamu
df_result1 <- df_refom |> 
  mutate(
    n_q_cl = str_to_lower(n_q_cl),
    col_cl = case_when(
      str_starts(n_q_cl, "v\\d") & year <= 2006 ~ n_q_cl,
      T ~ col_cl
    ),
    n_q_cl = ifelse(str_detect(n_q_cl, "^\\d"), "", n_q_cl),
    n_q_cl = ifelse(str_detect(col_cl,  fixed(n_q_cl)), "", n_q_cl),
    n_q_cl = ifelse(str_detect(n_q_cl,  "cuadro"), "", n_q_cl),
    desc_q_cl = case_when(
      desc_q_cl == "" & str_detect(n_q_cl, " ") ~ n_q_cl,
      T ~ desc_q_cl
    ),
    n_q_cl = ifelse(n_q_cl == desc_q_cl, "", n_q_cl),
    
    values = str_replace_all(values, "\\n", "; "),
    col_cl = case_when(
      !str_starts(col_cl, "p|c") & str_starts(n_q_cl, "p|c") & year >= 2007 ~ n_q_cl,
      T ~ col_cl
    ),
    n_q_cl = ifelse(str_detect(col_cl,  fixed(n_q_cl)), "", n_q_cl),
  ) |> 
  arrange(id) |> 
  mutate(
    h2 = ifelse(!str_detect(str_to_lower(h2), 'cuadro'), "", h2),
    desc_q_cl = case_when(
      str_trim(desc_q_cl) == "" & str_detect(h2, " ") ~ h2,
      T ~ desc_q_cl
      ),
    h2 = ifelse(h2 == desc_q_cl, "", h2),
  ) |> 
  # si se quiere revisar el raw hasta lo anterior
  select(
    !c(h2, n_q, desc_q, col, desc_col, n_col_cl, n_c)
  )
  

# Función para limpiar caracteres especiales del español
limpiar_especiales <- function(x) {
  x |>
    stri_trans_general("Latin-ASCII") |>  # á->a, é->e, ñ->n, etc.
    str_replace_all("[^a-zA-Z0-9\\s]", " ") |>  # elimina no alfanuméricos
    str_squish()  # espacios múltiples -> 1
}


# columnas limpias (lo que se pudo )
df_result2 <- 
  df_result1 |> 
  mutate(
    c = ifelse(str_detect(c, "^\\d"), "", c),
    c = ifelse(year >= 2021, "", c),
    c1 = str_extract(cuadro, "(?i)CUADRO_\\s*\\w+\\d"),
    cuadro = str_remove(cuadro, "(?i)CUADRO_\\s*\\w+\\d"),
    desc_q_cl = str_remove(desc_q_cl, "(?i)CUADRO_\\s*\\w+\\d"),
    cuadro = str_remove_all(cuadro, "\\|"),
    c = ifelse(c == "", c1, c),
    c = str_extract(c, "\\d+") |> as.integer(),
    col_cl = case_when(
      year == 2012 & str_detect(col_cl, 'total') & str_starts(n_q_cl, "p") ~ n_q_cl,
      T ~ col_cl
    )
  ) |> 
  select(!c(cuadro, c1, n_q_cl)) |> 
  relocate(
    id, year, modulo, page, row_id_page = row_id, cuadro = c, desc_q_cl, 
    col_cl, desc_col_cl, values, values_afirmative = value_pos
  ) |> 
  mutate(
    col = col_cl, desc_columna = desc_col_cl, desc_cuadro_pregunta = desc_q_cl
  ) |> 
  arrange(id) |> 
  mutate(
    # ── desc_cuadro_pregunta ──────────────────────────────────────────
    desc_cuadro_pregunta = stri_trans_general(desc_cuadro_pregunta, "Latin-ASCII") |>
      str_replace_all("\\s{2,}", " ") |>
      str_trim(),
    
    c_years_ref = sapply(
      str_extract_all(desc_cuadro_pregunta, "2\\s*0\\s*\\d\\s*\\d"),
      function(x) paste(unique(str_remove_all(x, "\\s")), collapse = ";")
    ),
    
    c_acronimos_ref = sapply(
      str_extract_all(desc_cuadro_pregunta, "(?<=\\()([^)]+)(?=\\))"),
      function(x) if (length(x) == 0) NA_character_ else paste(x, collapse = ";")
    ),
    
    desc_cuadro_pregunta = desc_cuadro_pregunta |>
      str_remove_all("2\\s*0\\s*\\d\\s*\\d") |>
      str_remove_all("\\([^)]*\\)") |>
      limpiar_especiales(),
    
    # ── desc_columna ──────────────────────────────────────────────────
    desc_columna = stri_trans_general(desc_columna, "Latin-ASCII") |>
      str_replace_all("\\s{2,}", " ") |>
      str_trim(),
    
    col_acronimos_ref = sapply(
      str_extract_all(desc_columna, "(?<=\\()([^)]+)(?=\\))"),
      function(x) if (length(x) == 0) NA_character_ else paste(x, collapse = ";")
    ),
    
    col_years_ref = sapply(
      str_extract_all(desc_columna, "2\\s*0\\s*\\d\\s*\\d"),
      function(x) paste(unique(str_remove_all(x, "\\s")), collapse = ";")
    ),
    
    desc_columna = desc_columna |>
      str_remove_all("2\\s*0\\s*\\d\\s*\\d") |>
      str_remove_all("\\([^)]*\\)") |>
      str_squish(),
    
    # ── col ───────────────────────────────────────────────────────────
    col_num = col |>
      str_remove_all("\\s") |>
      str_extract("\\d+") |>
      as.integer(),
    
    col_num = if_else(col_num > 2006, col_num, NA_integer_)
  )
  

library(DBI)
library(RSQLite)

sql_lite <- file.path(INPUT_ROOT, "clasification", "main.db")


con <- dbConnect(SQLite(), sql_lite)

df_variables <- 
  df_result2 |> 
  distinct(year, desc_cuadro_pregunta, desc_columna) |> 
  mutate(id_group = row_number())


df_columnas <- 
  df_result2 |> 
  inner_join(df_variables)
df_variables <- df_variables |> 
  mutate(categoria = "", subcategoria="", classified_at = "", classified_by = "")
# dbWriteTable(con, "renamu_variables", df_variables, overwrite  = TRUE)

# dbWriteTable(con, "renamu_columnas", df_columnas, overwrite  = TRUE)





