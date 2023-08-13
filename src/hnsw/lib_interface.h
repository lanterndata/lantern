#ifndef LDB_HNSW_LIB_INTERFACE_H
#define LDB_HNSW_LIB_INTERFACE_H
#ifdef __cplusplus
extern "C" {
#endif
// these are implemented by hnswlib third party dependency
// the goal is to make the interface generic over other vector index providers
typedef void* hnsw_t;
hnsw_t        hnsw_new(int dimension, int max_elements, int M, int ef_construction);
hnsw_t        hnsw_new_brute(int dimension, int max_elements);
void          hnsw_add(hnsw_t hnsw, float* vector, long unsigned int label);
void          hnsw_search(
             hnsw_t hnsw, float* vector, int k, int* out_num_returned, float* out_distances, long unsigned int* out_labels);
int hnsw_size(hnsw_t hnsw);
// temporary, before I understand WAL and buffer stuff of postgres
void   hnsw_save(hnsw_t hnsw, const char* filename);
hnsw_t hnsw_load(const char* filename, int dimension, int max_elements);

void hnsw_destroy(hnsw_t hnsw);

#ifdef __cplusplus
}
#endif
#endif  // LDB_HNSW_LIB_INTERFACE_H
