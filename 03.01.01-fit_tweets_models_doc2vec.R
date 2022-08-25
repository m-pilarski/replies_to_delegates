# TODO:
# sentence-transformers
# https://www.sbert.net/
# Batchwise UMAP
# https://github.com/marouabahri/UMAP-kNN
# collapse topics
# https://github.com/ddangelov/Top2Vec/blob/d625b507aa18a921b7d0a3710d1a4c176f9b8f84/top2vec/Top2Vec.py#L1777


################################################################################

setwd("~/Documents/get_tweets/")

################################################################################

library(reticulate)
library(tidyverse)
library(tidygraph)
library(ggraph)

################################################################################
################################################################################
################################################################################

dir_data <- dir_create("~/Documents/get_tweets/data")
dir_temp <- dir_create(path(dir_data, "temp"))

################################################################################
################################################################################
################################################################################

local({
  
  .conda_name <- "replies_to_delegates"
  
  .pkg_need <- c(
    "gensim", "umap-learn", "pynndescent", "hdbscan", "joblib", "sklearn",
    "pandas", "networkx", "matplotlib"
  )
  
  if(!.conda_name %in% reticulate::conda_list()$name){
    reticulate::conda_create(.conda_name)
  }
  
  reticulate::use_condaenv(.conda_name)
  
  .pgk_have <- reticulate::py_list_packages(envname=.conda_name)$package
  .pkg_miss <- .pkg_need[!.pkg_need %in% .pkg_have]
  
  if(length(.pkg_miss) > 0){
    reticulate::conda_install(
      envname=.conda_name, packages=py_packages_miss, pip=TRUE
    )
  }
  
})

################################################################################

pybi <- reticulate::import_builtins()
gens <- reticulate::import("gensim")
umap <- reticulate::import("umap")
hdbs <- reticulate::import("hdbscan")
skln <- reticulate::import("sklearn")
jobl <- reticulate::import("joblib")
logg <- reticulate::import("logging")

################################################################################

logg$basicConfig(
  format='%(asctime)s : %(levelname)s : %(message)s', level=logg$INFO
)

################################################################################

networkx_to_tidygraph <- function(.networkx_graph) {
  
  .node_data <- 
    .networkx_graph$nodes(data=TRUE) %>% 
    reticulate::iterate(function(..node){
      c(list(name=..node[[1]]), ..node[[2]])
    }) %>% 
    bind_rows() %>% 
    mutate(across(name, as.character))
  
  .edge_data <- 
    .networkx_graph$edges(data=TRUE) %>% 
    reticulate::iterate(function(..edge){
      c(list(from=..edge[[1]], to=..edge[[2]]), ..edge[[3]])
    }) %>% 
    bind_rows() %>% 
    mutate(across(c(from, to), as.character))
  
  .tidygraph_graph <- tidygraph::tbl_graph(
    nodes=.node_data, edges=.edge_data, directed=.networkx_graph$is_directed(), 
    node_key="name"
  )
  
  gc()
  
  return(.tidygraph_graph)

}

################################################################################
################################################################################
################################################################################

docs_doc2vec_mod <- gens$models$Doc2Vec$load("./docs_doc2vec_mod.sav")

docs_doc2vec_mod <- gens$models$Doc2Vec(
  vector_size=300L, min_count=0L, window=15L, dm=0L, dbow_words=1L,
  hs=1L, negative=0L, seed=126L, workers=5L
)

docs_doc2vec_mod$build_vocab(
  corpus_file=path(dir_data, "tweets_clean_plain", ext="txt")
)

docs_doc2vec_mod$train(
  corpus_file=path(dir_data, "tweets_clean_plain", ext="txt"), 
  epochs=20L,
  total_examples=docs_doc2vec_mod$corpus_count,
  total_words=docs_doc2vec_mod$corpus_total_words,
  # alpha=0.001
)

# docs_doc2vec_mod$save("./docs_doc2vec_mod.sav")

################################################################################

docs_doc2vec_simil_sample <- sample(
  as.integer(seq_along(docs_doc2vec_mod$docvecs)), size=20
)

docs_doc2vec_mod$docvecs %>%
  seq_along() %>%
  as.integer() %>%
  sample(size=20) %>%
  as_tibble_col("doc_a") %>%
  mutate(doc_b = map_int(doc_a, function(.doc_a){
    docs_doc2vec_mod$docvecs$most_similar(.doc_a - 1L, topn=1L)[[1]][[1]] + 1L
  })) %>%
  mutate(across(everything(), function(.idx){
    read_lines("tagged_docs.txt")[.idx]
  })) %>%
  gt::gt()

################################################################################
gc() ###########################################################################
################################################################################

docs_subset <- sample(
  as.integer(seq_along(docs_doc2vec_mod$docvecs)), size=5e5
)

################################################################################
gc() ###########################################################################
################################################################################

docs_umap_2d_mod <- umap$UMAP(
  n_neighbors=50L, n_components=2L, metric="cosine", min_dist=0, 
  low_memory=TRUE, verbose=TRUE, n_epochs=400L
)

docs_umap_2d_embed <- docs_umap_2d_mod$fit_transform(
  docs_doc2vec_mod$docvecs[docs_subset - 1L]
)

docs_umap_2d_embed %>%
  magrittr::set_colnames(c("x", "y")) %>%
  as_tibble() %>%
  ggplot(aes(x=x, y=y)) +
  geom_point(size=0.1, alpha=0.2)

################################################################################
gc() ###########################################################################
################################################################################

docs_umap_5d_mod <- umap$UMAP(
  n_neighbors=50L, n_components=5L, metric="cosine", min_dist=0, 
  low_memory=TRUE, verbose=TRUE, n_epochs=400L
)

docs_umap_5d_embed <- docs_umap_5d_mod$fit_transform(
  docs_doc2vec_mod$docvecs[docs_subset - 1L]
)

jobl$dump(docs_umap_5d_mod, "docs_umap_5d_mod.sav")

################################################################################
gc() ###########################################################################
################################################################################

docs_umap_5d_norm_mod <- skln$preprocessing$Normalizer(norm="l2")

docs_umap_5d_norm_mod$fit(docs_umap_5d_embed)

docs_umap_5d_embed_norm <- docs_umap_5d_norm_mod$transform(docs_umap_5d_embed)

jobl$dump(docs_umap_norm_mod, "docs_umap_norm_mod.sav")

################################################################################
gc() ###########################################################################
################################################################################

# docs_hdbscan_search_grid <- 
#   expand_grid(
#     min_cluster_size=c(
#       seq(10, 50, by=5), seq(60, 90, by=10), seq(100, 600, by=50)
#     )
#   ) %>% 
#   group_by(across(everything())) %>% 
#   group_modify(function(.gdata, .gkeys){
#     
#     .docs_hdbscan_search_mod <- hdbs$HDBSCAN(
#       min_cluster_size=as.integer(.gkeys$min_cluster_size), 
#       metric="euclidean", cluster_selection_method="eom",
#       memory=fs::dir_create(fs::file_temp()), prediction_data=TRUE, 
#       gen_min_span_tree=TRUE
#     )
#     
#     .docs_hdbscan_search_mod$fit(docs_umap_embed_norm)
#     
#     .docs_hdbscan_search_eval <- tibble(
#       relative_validity = .docs_hdbscan_search_mod$relative_validity_
#     )
#     
#     print(bind_cols(.docs_hdbscan_search_eval, .gkeys))
#     
#     gc()
#     
#     return(.docs_hdbscan_search_eval)
#     
#   }) %>% 
#   ungroup()

################################################################################
gc() ###########################################################################
################################################################################

# docs_hdbscan_mod <- jobl$load("docs_hdbscan_mod.sav")

docs_hdbscan_mod <- hdbs$HDBSCAN(
  min_cluster_size=250L, #min_samples=1L, cluster_selection_epsilon=1,
  metric="euclidean", cluster_selection_method="eom",
  memory=fs::dir_create(fs::file_temp()), 
  prediction_data=TRUE, gen_min_span_tree=TRUE
)

docs_hdbscan_mod$fit(docs_umap_5d_embed_norm)

docs_hdbscan_mod$relative_validity_

jobl$dump(docs_hdbscan_mod, "docs_hdbscan_mod.sav")

docs_hdbscan_clust <- hdbs$approximate_predict(
  clusterer=docs_hdbscan_mod, points_to_predict=docs_umap_5d_embed_norm
)

################################################################################

sort(table(docs_hdbscan_clust[[1]]), descending=TRUE)

mean(docs_hdbscan_clust[[1]] == -1)

################################################################################
gc() ###########################################################################
################################################################################

docs_umap_2d_embed %>%
  magrittr::set_colnames(c("x", "y")) %>%
  as_tibble() %>% 
  add_column(cluster = na_if(docs_hdbscan_clust[[1]], -1)) %>% 
  arrange(if_else(is.na(cluster), 0, 1)) %>% 
  ggplot(aes(x=x, y=y, color=fct_shuffle(factor(cluster)))) +
  geom_point(size=0.1, alpha=0.2) +
  scale_color_viridis_d(option="magma", na.value="gray70") +
  guides(color=guide_legend(override.aes=list(alpha=1, size=1))) +
  theme_void() +
  theme(legend.position="none")

################################################################################
gc() ###########################################################################
################################################################################

docs_hdbscan_clust %>% 
  set_names(c("cluster_id", "cluster_prob")) %>% 
  bind_cols() %>% 
  add_column(doc_id = docs_subset, .after=0) %>% 
  group_by(cluster_id) %>% 
  slice_max(order_by=cluster_prob, n=10, with_ties=FALSE) %>% 
  ungroup() %>% 
  mutate(doc_text=read_lines("tagged_docs.txt")[doc_id]) %>% 
  gt::gt()

################################################################################

docs_hdbscan_mod$condensed_tree_$to_pandas() %>% 
  as_tibble() %>% 
  filter(child_size > 1) %>%
  select(from=parent, to=child, everything()) %>% # summary()
  mutate(across(c(from, to), `-`, length(docs_subset))) %>% #view
  as_tbl_graph() %>%  
  activate(nodes) %>% 
  mutate(
    node_id = row_number(),
    size=map2_dbl(node_id, list(.E()), function(.from, .edge_data){
      sum(filter(.edge_data, from == .from)$child_size)
    }),
    lambda_val=map2_dbl(node_id, list(.E()), function(.to, .edge_data){
      unique(pluck(filter(.edge_data, to == .to), "lambda_val", .default=0))
    }),
    node_is_cluster = name %in% as.character(docs_hdbscan_clust[[1]])
  ) %>% 
  filter(node_is_cluster) %>% 
  activate(edges) %>% 
  mutate() %>% 
  create_layout("dendrogram", height=-(lambda_val^(1/4)), circular=TRUE) %>%
  ggraph() +
  geom_edge_elbow(aes(edge_width=child_size)) +
  geom_node_point(aes(size=size, color=node_is_cluster)) + 
  guides(size="none", edge_width="none") +
  scale_size_continuous(range=c(1, 5)) +
  scale_edge_width_continuous(range=c(0.5, 2.5)) +
  coord_fixed() +
  theme(legend.position="bottom")

################################################################################
################################################################################
################################################################################